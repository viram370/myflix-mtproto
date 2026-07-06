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
    try {
        telegramClient = await startClient();

        app.locals.telegramClient = telegramClient;

        const PORT = process.env.PORT || 3000;

        app.listen(PORT, () => {
            console.log(`🚀 Server running on port ${PORT}`);
        });

    } catch (err) {
        console.error(err);
    }
}

start();
