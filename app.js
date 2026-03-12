require("dotenv").config();

const express = require("express");
const http = require("http");
const { twilioRouter } = require("./Twilio/voice-route");
const { setupVoiceWebSocket } = require("./Twilio/websocket");

const { sarvamRouter } = require("./Sarvam/sarvam-route");

const app = express();
const PORT = process.env.PORT || 3000;

app.use(express.urlencoded({ extended: false }));
app.set("trust proxy", 1);

app.use("/voice", twilioRouter);
app.use("/sarvam", sarvamRouter);

app.get("/", (req, res) => {
    res.send("Agri-Agents API is running. Voice routes are under /voice.");
});

const server = http.createServer(app);
setupVoiceWebSocket(server, { wsPath: "/voice/stream" });

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Server running on port ${PORT}`);
    console.log(`Local URL: http://localhost:${PORT}`);
    console.log(`Voice webhook: http://localhost:${PORT}/voice/incoming`);
    console.log(`Voice stream: ws://localhost:${PORT}/voice/stream`);
});
