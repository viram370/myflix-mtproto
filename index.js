require("dotenv").config();
app.use("/api/stream", require("./routes/stream"));
const express = require("express");
const cors = require("cors");

const { startClient } = require("./gramjs");

const app = express();

app.use(cors());
app.use(express.json());

// Health Check
app.get("/", (req, res) => {
    res.json({
        status: "ok",
        message: "MYFLIX MTProto Server Running"
    });
});

const PORT = process.env.PORT || 3000;

// Start Express FIRST
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
});

// Connect Telegram AFTER server starts
startClient()
    .then((client) => {
        app.locals.telegramClient = client;
        console.log("✅ Telegram Connected");
    })
    .catch((err) => {
        console.error("❌ Telegram Connection Error:", err);
    });

// Prevent app crash
process.on("uncaughtException", (err) => {
    console.error("Uncaught Exception:", err);
});

process.on("unhandledRejection", (err) => {
    console.error("Unhandled Rejection:", err);
});
