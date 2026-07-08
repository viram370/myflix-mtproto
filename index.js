require("dotenv").config();

const express = require("express");
const cors = require("cors");

const { startClient } = require("./gramjs");

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// Stream Route
app.use("/api/stream", require("./routes/stream"));

// Health Check
app.get("/", (req, res) => {
    res.json({
        status: "ok",
        message: "MYFLIX MTProto Server Running"
    });
});

// Root Page
app.get("/", (req, res) => {
    res.send("MYFLIX MTProto Server Running");
});

const PORT = process.env.PORT || 3000;

// Start Express Server
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
});

// Start Telegram Client
(async () => {
    try {
        const client = await startClient();

        app.locals.telegramClient = client;

        console.log("✅ Telegram Connected");
    } catch (err) {
        console.error("❌ Telegram Connection Error:", err);
    }
})();

// Handle uncaught errors
process.on("uncaughtException", (err) => {
    console.error("Uncaught Exception:", err);
});

process.on("unhandledRejection", (err) => {
    console.error("Unhandled Rejection:", err);
});

process.on("SIGINT", () => {
    console.log("Stopping server...");
    process.exit(0);
});

process.on("SIGTERM", () => {
    console.log("Stopping server...");
    process.exit(0);
});
