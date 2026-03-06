/**
 * Bedrock ConverseStream service - streaming text responses from foundation models.
 */

const {
    BedrockRuntimeClient,
    ConverseStreamCommand,
    ConversationRole,
} = require("@aws-sdk/client-bedrock-runtime");

// Use Claude 3 Haiku (on-demand) or set BEDROCK_MODEL_ID for inference profile (e.g. us.anthropic.claude-sonnet-4-6)
const DEFAULT_MODEL = process.env.BEDROCK_MODEL_ID || "anthropic.claude-3-haiku-20240307-v1:0";
const SYSTEM_PROMPT =
    "You are a voice assistant for Indian farmers. Answer crop, weather, and farming questions. " +
    "CRITICAL: Keep responses to 1–2 short sentences. Max 2 sentences. No bullet points, no lists, no long explanations. Be direct and conversational like a call-center advisor. " +
    "CRITICAL: Respond in the same language the user speaks. Hindi/Hinglish -> reply in Hindi/Hinglish. English -> English. Never switch to Spanish or other languages.";

let client = null;

function getClient() {
    if (client) return client;
    client = new BedrockRuntimeClient({
        region: process.env.AWS_REGION || "us-east-1",
    });
    return client;
}

/**
 * Convert conversation history to Bedrock message format.
 * @param {Array<{role: string, content: string}>} history - [{ role, content }]
 */
function toBedrockMessages(history) {
    return history.map(({ role, content }) => ({
        role: role === "user" ? ConversationRole.USER : ConversationRole.ASSISTANT,
        content: [{ text: content }],
    }));
}

/**
 * Stream a response from Bedrock given user transcript and conversation history.
 * @param {string} userMessage - The user's transcript (latest utterance)
 * @param {Array<{role: string, content: string}>} conversationHistory - Prior turns
 * @param {AbortSignal} [abortSignal] - Abort when call ends
 * @returns {AsyncGenerator<string>} Yields text chunks as they stream
 */
async function* generateResponseStream(userMessage, conversationHistory = [], abortSignal = null) {
    const messages = toBedrockMessages(conversationHistory);
    messages.push({
        role: ConversationRole.USER,
        content: [{ text: userMessage }],
    });

    if (process.env.DEBUG_LLM_PROMPT) {
        console.log("   [LLM] full prompt:", JSON.stringify({ messages, system: SYSTEM_PROMPT }, null, 2));
    }

    const command = new ConverseStreamCommand({
        modelId: DEFAULT_MODEL,
        messages,
        system: [{ text: SYSTEM_PROMPT }],
        inferenceConfig: {
            maxTokens: 120,
            temperature: 0.2,
            topP: 0.7,
        },
    });

    const sendOptions = abortSignal ? { abortSignal } : {};
    const response = await getClient().send(command, sendOptions);

    if (!response.stream) {
        throw new Error("No stream in ConverseStream response");
    }

    let chunkCount = 0;
    for await (const event of response.stream) {
        if (event.contentBlockDelta?.delta?.text) {
            chunkCount++;
            yield event.contentBlockDelta.delta.text;
        }
    }
}

module.exports = {
    generateResponseStream,
};
