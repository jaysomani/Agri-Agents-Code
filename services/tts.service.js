/**
 * Sarvam TTS service - convert text to speech (PCM audio for Twilio).
 * Includes retry, min-length guard, and sequential queue for production stability.
 */

const sarvamConfig = require("../Sarvam/sarvam.config");
const SarvamAIClient = require("sarvamai").SarvamAIClient;

const SAMPLE_RATE = 8000; // Twilio expects 8kHz
const DEFAULT_LANGUAGE = "en-IN";
const MIN_TTS_WORDS = 5;
const MAX_RETRIES = 2;
const RETRY_DELAY_MS = 500;

let sarvamClient = null;

function getClient() {
    if (!sarvamConfig.sarvamApiKey) return null;
    if (sarvamClient) return sarvamClient;
    sarvamClient = new SarvamAIClient({ apiSubscriptionKey: sarvamConfig.sarvamApiKey });
    return sarvamClient;
}

/**
 * Raw TTS call with retry and backoff. Never throws - returns null on permanent failure.
 */
async function textToSpeechWithRetry(text, languageCode = DEFAULT_LANGUAGE, retries = MAX_RETRIES) {
    const client = getClient();
    if (!client) {
        console.error("   [TTS] API key not configured");
        return null;
    }

    const clean = String(text || "").trim();
    const wordCount = clean.split(/\s+/).filter(Boolean).length;
    if (wordCount < MIN_TTS_WORDS) {
        console.log("   [TTS] SKIP - too short:", wordCount, "words");
        return null;
    }

    for (let attempt = 0; attempt <= retries; attempt++) {
        try {
            const response = await client.textToSpeech.convert({
                text: clean,
                target_language_code: languageCode,
                model: "bulbul:v3",
                speaker: "shubh",
                output_audio_codec: "linear16",
                speech_sample_rate: SAMPLE_RATE,
            });

            const audios = response?.audios ?? response?.data?.audios;
            if (!audios?.length) {
                console.error("   [TTS] No audio in response");
                return null;
            }
            return Buffer.from(audios[0], "base64");
        } catch (err) {
            const status = err?.statusCode ?? err?.status ?? "?";
            console.error("   [TTS] attempt", attempt + 1, "failed:", err?.message || err, "| status:", status);
            if (attempt < retries) {
                await new Promise((r) => setTimeout(r, RETRY_DELAY_MS));
            } else {
                console.error("   [TTS] All retries exhausted, skipping segment");
                return null;
            }
        }
    }
    return null;
}

/** Sequential queue - TTS runs one at a time globally to avoid burst/rate-limit */
let ttsQueue = Promise.resolve();

/**
 * Enqueue TTS - runs sequentially. Returns null on failure (never throws).
 */
async function textToSpeech(text, languageCode = DEFAULT_LANGUAGE) {
    return new Promise((resolve) => {
        ttsQueue = ttsQueue
            .then(() => textToSpeechWithRetry(text, languageCode))
            .then(resolve)
            .catch((e) => {
                console.error("   [TTS] Queue error:", e?.message);
                resolve(null);
            });
    });
}

module.exports = {
    textToSpeech,
};
