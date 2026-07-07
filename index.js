require("dotenv").config();

const express = require("express");
const cors = require("cors");

const { startClient } = require("./gramjs");

const app = express();

app.use(cors());
app.use(express.json());

let telegramClient = null;

// Health Check
app.get("/", (req, res) => {
    res.json({
        status: "ok",
        message: "MYFLIX MTProto Server Running"
    });
});

// Start Server
async function start() {
    const PORT = process.env.PORT || 3000;

// Start Express FIRST
app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
});

// Connect Telegram AFTER Express starts
startClient()
    .then(client => {
        app.locals.telegramClient = client;
        console.log("✅ Telegram Connected");
    })
    .catch(err => {
        console.error("Telegram Error:", err);
    });
}

start();
