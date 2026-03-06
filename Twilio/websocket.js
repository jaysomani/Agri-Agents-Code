const fs = require("fs");
const path = require("path");
const { WebSocketServer } = require("ws");
const { mulawToWav, mulawToPcmBuffer, pcmToWavBuffer, pcmToMulawBuffer } = require("../Utils/Utility");
const { randomUUID } = require("crypto");
const SarvamAIClient = require("sarvamai").SarvamAIClient;
const sarvamConfig = require("../Sarvam/sarvam.config");
const { generateResponseStream } = require("../services/llm.service");
const { textToSpeech } = require("../services/tts.service");

const RAW_AUDIO_DIR = path.join(process.cwd(), "raw-recordings");
fs.mkdirSync(RAW_AUDIO_DIR, { recursive: true });

// ~200ms of 8kHz PCM = 3200 bytes. Twilio sends ~160 bytes/chunk (20ms) = 10 chunks
const SARVAM_BUFFER_MS = 200;
const PCM_BYTES_PER_MS = 16; // 8kHz * 2 bytes/sample
// After this much silence (no new transcript), assume user finished speaking and trigger pipeline
const SILENCE_TRIGGER_MS = 1200;

// Start TTS when we have this many words from LLM (reduces time-to-first-audio)
const TTS_FIRST_CHUNK_WORDS = 15;
// Min words per TTS segment - Sarvam can fail on tiny fragments
const TTS_MIN_WORDS = 5;

// Filler/short utterances - do not send to LLM (saves cost, avoids ghost replies)
const FILLER_WORDS = new Set([
    "okay", "ok", "hm", "hmm", "haan", "han", "yes", "no", "right", "aha",
    "uh", "um", "oh", "sure", "alright", "good", "fine", "thanks", "thank you",
]);
const MIN_UTTERANCE_LENGTH = 8;

const sarvamClient = sarvamConfig.sarvamApiKey
    ? new SarvamAIClient({ apiSubscriptionKey: sarvamConfig.sarvamApiKey })
    : null;

function isFillerOrTooShort(transcript) {
    if (!transcript || transcript.length < MIN_UTTERANCE_LENGTH) return true;
    const lower = transcript.trim().toLowerCase().replace(/[.!?,]+$/, "");
    if (FILLER_WORDS.has(lower)) return true;
    return false;
}

function createConnectionState() {
    return {
        connectionId: randomUUID(),
        callSid: "unknown",
        mediaCount: 0,
        totalBytes: 0,
        stopped: false,
        baseName: "",
        rawPath: "",
        wavPath: "",
        rawStream: null,
        ws: null,
        streamSid: null,
        abortController: null,
        // Sarvam real-time streaming
        sarvamSocket: null,
        pcmBuffer: [],
        pcmBufferBytes: 0,
        sarvamHadError: false,
        // LLM + TTS pipeline
        conversationHistory: [],
        lastTranscript: "",
        transcripts: [], // accumulate all transcripts; use longest on stop
        silenceTimer: null,
        pipelineProcessing: false,
    };
}

function ensureRawStream(state) {
    if (state.rawStream) return;

    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    state.baseName = `${timestamp}_${state.callSid || state.connectionId}`;
    state.rawPath = path.join(RAW_AUDIO_DIR, `${state.baseName}.mulaw`);
    state.wavPath = path.join(RAW_AUDIO_DIR, `${state.baseName}.wav`);
    state.rawStream = fs.createWriteStream(state.rawPath);
}

function endStream(stream) {
    return new Promise((resolve, reject) => {
        stream.end((err) => {
            if (err) reject(err);
            else resolve();
        });
    });
}

async function finalizeAndConvert(state) {
    if (state.rawStream) {
        await endStream(state.rawStream);
        state.rawStream = null;
    }

    if (!state.rawPath || !state.wavPath) return;

    const mulawBuffer = await fs.promises.readFile(state.rawPath);
    const wavBuffer = mulawToWav(mulawBuffer);
    await fs.promises.writeFile(state.wavPath, wavBuffer);
    await fs.promises.unlink(state.rawPath).catch(() => {});
}

function clearSilenceTimer(state) {
    if (state.silenceTimer) {
        clearTimeout(state.silenceTimer);
        state.silenceTimer = null;
    }
}

function triggerPipelineFromSilence(state) {
    clearSilenceTimer(state);
    if (state.stopped || state.pipelineProcessing) return;
    const toProcess = state.transcripts.length > 0
        ? state.transcripts.reduce((a, b) => (a.length >= b.length ? a : b))
        : state.lastTranscript;
    if (!toProcess?.trim() || isFillerOrTooShort(toProcess)) {
        if (toProcess?.trim()) console.log("   [Silence trigger] SKIP filler/short:", toProcess);
        return;
    }
    state.transcripts = [];
    state.pipelineProcessing = true;
    console.log("   [Silence trigger] running pipeline (user still on call), transcript:", toProcess.substring(0, 50) + (toProcess.length > 50 ? "..." : ""));
    handleUserUtterance(state, toProcess)
        .catch((e) => console.error("   [Pipeline] Error:", e?.message))
        .finally(() => { state.pipelineProcessing = false; });
}

/**
 * Send PCM audio to the caller via Twilio WebSocket (μ-law 8kHz).
 */
async function playWelcome(state) {
    if (!state.streamSid || state.stopped) return;
    try {
        const welcomeText = "Welcome to Agri Agents. Please tell me your question.";
        const audioBuffer = await textToSpeech(welcomeText);
        if (!audioBuffer || state.stopped) return;
        const buf = Buffer.isBuffer(audioBuffer) ? audioBuffer : Buffer.from(audioBuffer);
        const BYTES_PER_CHUNK = (8000 * 2 * 20) / 1000;
        for (let i = 0; i < buf.length && !state.stopped; i += BYTES_PER_CHUNK) {
            const chunk = buf.subarray(i, Math.min(i + BYTES_PER_CHUNK, buf.length));
            sendAudioToCaller(state, chunk);
        }
        console.log("   [Welcome] played to caller");
    } catch (err) {
        console.error("   [Welcome] TTS error:", err?.message);
    }
}

function sendAudioToCaller(state, pcmBuffer) {
    if (!state.ws || state.ws.readyState !== 1 || !state.streamSid || state.stopped) return;
    const mulawBuffer = pcmToMulawBuffer(pcmBuffer);
    try {
        state.ws.send(
            JSON.stringify({
                event: "media",
                streamSid: state.streamSid,
                media: { payload: mulawBuffer.toString("base64") },
            })
        );
    } catch (err) {
        console.error("   [Playback] send error:", err?.message);
    }
}

/**
 * On speech_end, silence timeout, or Sarvam close fallback: send transcript to LLM, TTS, play to caller.
 */
async function handleUserUtterance(state, transcript) {
    if (state.stopped || !transcript?.trim()) {
        console.log("   [Pipeline Step 0] SKIP - stopped:", state.stopped, "empty:", !transcript?.trim());
        return;
    }

    const userMessage = transcript.trim();

    state.conversationHistory.push({ role: "user", content: userMessage });

    const abortSignal = state.abortController?.signal;
    let fullResponse = "";
    const ttsSegments = [];
    try {
        console.log("   [Pipeline Step 2] Calling Bedrock LLM (generateResponseStream)...");
        let buffer = "";
        for await (const chunk of generateResponseStream(userMessage, state.conversationHistory, abortSignal)) {
            if (state.stopped) break;
            fullResponse += chunk;
            buffer += chunk;
            for (;;) {
                const trimmed = buffer.trim();
                const words = trimmed.split(/\s+/).filter(Boolean);
                const sentMatch = trimmed.match(/^(.+?[.!?])\s+/s);
                const segment = sentMatch
                    ? sentMatch[1].trim()
                    : words.length >= TTS_FIRST_CHUNK_WORDS
                        ? words.slice(0, TTS_FIRST_CHUNK_WORDS).join(" ")
                        : null;
                if (!segment) break;
                const segWords = segment.split(/\s+/).filter(Boolean).length;
                if (segWords >= TTS_MIN_WORDS) ttsSegments.push(segment);
                buffer = sentMatch ? trimmed.slice(sentMatch[0].length) : words.slice(TTS_FIRST_CHUNK_WORDS).join(" ");
            }
        }
        if (state.stopped) {
            console.log("   [Pipeline error] ABORTED - call ended during LLM");
            state.conversationHistory.pop();
            return;
        }
        const remainder = buffer.trim();
        if (remainder && remainder.split(/\s+/).filter(Boolean).length >= TTS_MIN_WORDS) ttsSegments.push(remainder);
        if (ttsSegments.length === 0 && fullResponse.trim()) ttsSegments.push(fullResponse.trim());
        
    } catch (err) {
        if (err?.name === "AbortError") {
            console.log("   [Pipeline Step 3] ABORTED");
            state.conversationHistory.pop();
            return;
        }
        console.error("   [Pipeline Step 3] LLM Error:", err?.message || err);
        state.conversationHistory.pop();
        return;
    }

    const assistantMessage = fullResponse.trim();
    if (!assistantMessage || state.stopped) {
        console.log("   [Pipeline Step 4] SKIP - empty or stopped");
        return;
    }
    console.log("   [Pipeline Step 4] Assistant:", assistantMessage);
    state.conversationHistory.push({ role: "assistant", content: assistantMessage });

    try {
        for (let i = 0; i < ttsSegments.length && !state.stopped; i++) {
            const audioBuffer = await textToSpeech(ttsSegments[i]);
            if (audioBuffer && state.ws && state.ws.readyState === 1 && state.streamSid && !state.stopped) {
                const CHUNK_MS = 20;
                const BYTES_PER_CHUNK = (8000 * 2 * CHUNK_MS) / 1000;
                const buf = Buffer.isBuffer(audioBuffer) ? audioBuffer : Buffer.from(audioBuffer);
                for (let j = 0; j < buf.length && !state.stopped; j += BYTES_PER_CHUNK) {
                    const chunk = buf.subarray(j, Math.min(j + BYTES_PER_CHUNK, buf.length));
                    sendAudioToCaller(state, chunk);
                }
            }
        }
        console.log("   [Pipeline Step 5] Played TTS to caller");
    } catch (err) {
        console.error("   [Pipeline Step 5] TTS Error:", err?.message || err);
    }
}

async function connectSarvamStreaming(state) {
    if (!sarvamClient) return;
    state.sarvamHadError = false;
    try {
        const socket = await sarvamClient.speechToTextStreaming.connect({
            "language-code": "en-IN",
            model: "saaras:v3",
            mode: "transcribe",
            sample_rate: 8000,
            high_vad_sensitivity: true,
            flush_signal: true,
            debug: false,
        });

        socket.on("open", () => {
            console.log("   [Sarvam WebSocket] open event fired");
        });

        socket.on("close", (event) => {
            const code = event?.code ?? "?";
            const reason = event?.reason ?? "(none)";
            console.log(
                "   [Sarvam WebSocket CLOSED] code:",
                code,
                "reason:",
                String(reason),
                "| lastTranscript:",
                state.lastTranscript ? `"${state.lastTranscript.substring(0, 40)}..."` : "(empty)",
                "| stopped:",
                state.stopped,
            );
            // Fallback: Sarvam may close (1000) after each utterance without sending speech_end.
            clearSilenceTimer(state);
            if (code === 1000 && !state.stopped && !state.pipelineProcessing) {
                const toProcess = state.transcripts.length > 0
                    ? state.transcripts.reduce((a, b) => (a.length >= b.length ? a : b))
                    : state.lastTranscript;
                if (toProcess?.trim() && !isFillerOrTooShort(toProcess)) {
                    state.transcripts = [];
                    console.log("   [Sarvam] close(1000) fallback -> triggering pipeline");
                    state.pipelineProcessing = true;
                    handleUserUtterance(state, toProcess)
                        .catch((e) => console.error("   [Pipeline] Error:", e?.message))
                        .finally(() => { state.pipelineProcessing = false; });
                }
            }
            state.sarvamSocket = null;
            if (!state.stopped && sarvamClient && code === 1000 && !state.sarvamHadError) {
                console.log("   [Sarvam] reconnecting for next utterance...");
                connectSarvamStreaming(state);
            }
        });

        socket.on("message", (data) => {
            const msg = typeof data === "string" ? JSON.parse(data) : data;
            const type = msg?.type;
            if (type === "error") {
                state.sarvamHadError = true;
                console.error("   [Sarvam error]:", msg?.data?.message ?? msg);
                return;
            }
            const transcript =
                msg?.data?.transcript ?? msg?.transcript ?? msg?.text;
            if (transcript) {
                state.lastTranscript = transcript;
                state.transcripts.push(transcript);
                console.log("   [Sarvam transcript]:", transcript);
                // Reset silence timer: after SILENCE_TRIGGER_MS with no new transcript, run pipeline (user still on call)
                clearSilenceTimer(state);
                if (!state.stopped && !state.pipelineProcessing) {
                    state.silenceTimer = setTimeout(() => triggerPipelineFromSilence(state), SILENCE_TRIGGER_MS);
                }
            }
            if (type === "speech_start") {
                console.log("   [Sarvam] speech_start -> clearing lastTranscript");
                state.lastTranscript = "";
            }
            if (type === "speech_end") {
                const toSend = transcript || state.lastTranscript;
                if (toSend?.trim() && !isFillerOrTooShort(toSend)) {
                    console.log("   [Sarvam] speech_end -> trigger pipeline");
                    handleUserUtterance(state, toSend).catch((e) =>
                        console.error("   [Pipeline] Error:", e?.message)
                    );
                } else if (toSend?.trim()) {
                    console.log("   [Sarvam] speech_end skipped (filler/short):", toSend);
                }
            }
        });

        socket.on("error", (err) => {
            console.error("   [Sarvam WebSocket error]:", err?.message || err);
        });

        await socket.waitForOpen();
        if (state.stopped) {
            socket.close();
            return;
        }
        state.sarvamSocket = socket;
        console.log("   Sarvam streaming connected");
        flushPcmBufferToSarvam(state);
    } catch (err) {
        console.error("   Failed to connect Sarvam streaming:", err?.message || err);
    }
}

function flushPcmBufferToSarvam(state) {
    if (!state.sarvamSocket || state.pcmBufferBytes === 0) return;
    const rs = state.sarvamSocket.readyState;
    const states = { 0: "CONNECTING", 1: "OPEN", 2: "CLOSING", 3: "CLOSED" };
    if (rs !== 1) {
        console.log(
            "   [Sarvam forward] skipping - socket not OPEN, readyState:",
            rs,
            "(",
            states[rs] || "?",
            ")",
        );
        return;
    }
    try {
        const combined = Buffer.concat(state.pcmBuffer);
        const wavBuffer = pcmToWavBuffer(combined);
        const base64Wav = wavBuffer.toString("base64");
        state.sarvamSocket.transcribe({
            audio: base64Wav,
            encoding: "audio/wav",
            sample_rate: 8000,
        });
        state.pcmBuffer = [];
        state.pcmBufferBytes = 0;
    } catch (err) {
        const nowState = state.sarvamSocket?.readyState ?? "null";
        console.error(
            "   [Sarvam forward error]:",
            err?.message || err,
            "| readyState at error:",
            nowState,
        );
    }
}

function setupVoiceWebSocket(server, { wsPath = "/voice/stream" } = {}) {
    const wss = new WebSocketServer({ server, path: wsPath });

        wss.on("connection", (ws, req) => {
        console.log(
            "WebSocket client connected from:",
            req.headers.origin || req.socket.remoteAddress,
        );
        const state = createConnectionState();
        state.ws = ws;
        state.abortController = new AbortController();

        ws.on("error", (err) => {
            console.error("WebSocket error:", err.message);
        });

        ws.on("message", (data) => {
            try {
                const msg = JSON.parse(data.toString());

                switch (msg.event) {
                    case "connected":
                        console.log("Media Stream: WebSocket connected");
                        break;
                    case "start":
                        state.callSid = msg.start?.callSid || msg.streamSid || "unknown";
                        state.streamSid = msg.start?.streamSid || msg.streamSid;
                        console.log(
                            "   Media Stream started, Call SID:",
                            state.callSid,
                            "| streamSid:",
                            state.streamSid ? state.streamSid.substring(0, 12) + "..." : "?",
                        );
                        ensureRawStream(state);
                        connectSarvamStreaming(state);
                        playWelcome(state);
                        break;
                    case "media":
                        if (msg.media?.payload) {
                            ensureRawStream(state);
                            const mulawChunk = Buffer.from(
                                msg.media.payload,
                                "base64",
                            );
                            state.rawStream.write(mulawChunk);
                            state.mediaCount++;
                            state.totalBytes += mulawChunk.length;

                            if (sarvamClient) {
                                const pcmChunk = mulawToPcmBuffer(mulawChunk);
                                state.pcmBuffer.push(pcmChunk);
                                state.pcmBufferBytes += pcmChunk.length;
                                const bufferMs =
                                    (state.pcmBufferBytes / PCM_BYTES_PER_MS) | 0;
                                if (
                                    state.sarvamSocket &&
                                    bufferMs >= SARVAM_BUFFER_MS
                                ) {
                                    flushPcmBufferToSarvam(state);
                                }
                            }
                        }
                        break;
                    case "stop":
                        if (state.stopped) break;
                        clearSilenceTimer(state);
                        state.stopped = true;
                        if (state.abortController) {
                            state.abortController.abort();
                            console.log("   [Twilio stop] call ended - aborted in-flight LLM, no new pipeline");
                        }

                        if (state.sarvamSocket) {
                            flushPcmBufferToSarvam(state);
                            try {
                                state.sarvamSocket.flush();
                                state.sarvamSocket.close();
                            } catch (e) {
                                /* ignore close errors */
                            }
                            state.sarvamSocket = null;
                        }

                        finalizeAndConvert(state)
                            .then(() => {
                                console.log("Media Stream stopped");
                                console.log(
                                    "   Total chunks:",
                                    state.mediaCount,
                                    "| Raw:",
                                    state.totalBytes,
                                    "bytes (mulaw)",
                                );
                                if (state.wavPath) {
                                    console.log("   Saved WAV:", state.wavPath);
                                }
                            })
                            .catch((writeErr) => {
                                console.error(
                                    "Failed to save file:",
                                    writeErr.message,
                                );
                            });
                        break;
                    default:
                        console.log("WebSocket event:", msg.event);
                        break;
                }
            } catch (err) {
                console.error("WebSocket message error:", err.message);
            }
        });

        ws.on("close", () => {
            state.stopped = true;
            if (state.abortController) state.abortController.abort();
            clearSilenceTimer(state);
            if (state.rawStream) {
                state.rawStream.end();
                if (state.rawPath) {
                    fs.promises.unlink(state.rawPath).catch(() => {});
                }
            }
            if (state.sarvamSocket) {
                try {
                    state.sarvamSocket.close();
                } catch (e) {
                    /* ignore */
                }
                state.sarvamSocket = null;
            }
            console.log("WebSocket closed");
        });
    });

    return wss;
}

module.exports = {
    setupVoiceWebSocket,
};
