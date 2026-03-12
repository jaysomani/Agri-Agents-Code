const alawmulaw = require("alawmulaw");

function mulawToWav(mulawBuffer) {
    const mulawArray = new Uint8Array(mulawBuffer);
    const pcmSamples = alawmulaw.mulaw.decode(mulawArray);
    const numSamples = pcmSamples.length;
    const dataSize = numSamples * 2;
    const headerSize = 44;
    const fileSize = headerSize + dataSize;
    const buffer = Buffer.alloc(fileSize);
    let offset = 0;

    buffer.write("RIFF", offset);
    offset += 4;
    buffer.writeUInt32LE(fileSize - 8, offset);
    offset += 4;
    buffer.write("WAVE", offset);
    offset += 4;
    buffer.write("fmt ", offset);
    offset += 4;
    buffer.writeUInt32LE(16, offset);
    offset += 4;
    buffer.writeUInt16LE(1, offset);
    offset += 2;
    buffer.writeUInt16LE(1, offset);
    offset += 2;
    buffer.writeUInt32LE(8000, offset);
    offset += 4;
    buffer.writeUInt32LE(16000, offset);
    offset += 4;
    buffer.writeUInt16LE(2, offset);
    offset += 2;
    buffer.writeUInt16LE(16, offset);
    offset += 2;
    buffer.write("data", offset);
    offset += 4;
    buffer.writeUInt32LE(dataSize, offset);
    offset += 4;

    for (let i = 0; i < numSamples; i++) {
        buffer.writeInt16LE(pcmSamples[i], offset);
        offset += 2;
    }

    return buffer;
}

/**
 * Converts a μ-law audio chunk to raw PCM (16-bit signed, little-endian).
 * Used for real-time streaming to Sarvam (Twilio sends μ-law @ 8kHz).
 */
function mulawToPcmBuffer(mulawBuffer) {
    const mulawArray = new Uint8Array(mulawBuffer);
    const pcmSamples = alawmulaw.mulaw.decode(mulawArray);
    const buffer = Buffer.alloc(pcmSamples.length * 2);
    for (let i = 0; i < pcmSamples.length; i++) {
        buffer.writeInt16LE(pcmSamples[i], i * 2);
    }
    return buffer;
}

/**
 * Wraps raw PCM (16-bit LE, 8kHz mono) in a WAV header.
 * Sarvam streaming requires encoding: "audio/wav".
 */
function pcmToWavBuffer(pcmBuffer) {
    const dataSize = pcmBuffer.length;
    const headerSize = 44;
    const fileSize = headerSize + dataSize;
    const buffer = Buffer.alloc(fileSize);
    let offset = 0;

    buffer.write("RIFF", offset);
    offset += 4;
    buffer.writeUInt32LE(fileSize - 8, offset);
    offset += 4;
    buffer.write("WAVE", offset);
    offset += 4;
    buffer.write("fmt ", offset);
    offset += 4;
    buffer.writeUInt32LE(16, offset);
    offset += 4;
    buffer.writeUInt16LE(1, offset);
    offset += 2;
    buffer.writeUInt16LE(1, offset);
    offset += 2;
    buffer.writeUInt32LE(8000, offset);
    offset += 4;
    buffer.writeUInt32LE(16000, offset);
    offset += 4;
    buffer.writeUInt16LE(2, offset);
    offset += 2;
    buffer.writeUInt16LE(16, offset);
    offset += 2;
    buffer.write("data", offset);
    offset += 4;
    buffer.writeUInt32LE(dataSize, offset);
    offset += 4;
    pcmBuffer.copy(buffer, offset);
    return buffer;
}

/**
 * Convert raw PCM (16-bit LE, 8kHz mono) to μ-law for Twilio playback.
 * @param {Buffer} pcmBuffer - 16-bit signed little-endian PCM
 * @returns {Buffer} μ-law encoded buffer
 */
function pcmToMulawBuffer(pcmBuffer) {
    const numSamples = pcmBuffer.length / 2;
    const samples = new Int16Array(numSamples);
    for (let i = 0; i < numSamples; i++) {
        samples[i] = pcmBuffer.readInt16LE(i * 2);
    }
    const mulawArray = alawmulaw.mulaw.encode(samples);
    return Buffer.from(mulawArray);
}

module.exports = {
    mulawToWav,
    mulawToPcmBuffer,
    pcmToWavBuffer,
    pcmToMulawBuffer,
};
