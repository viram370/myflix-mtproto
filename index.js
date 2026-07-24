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

// Videos Route (metadata lookup)
// FIX: this router was fully implemented in routes/videos.js but was
// never mounted here, so every request to /api/videos/:id 404'd
// unconditionally (Express has no route for it at all - not even a
// controller-level 404, just "Cannot GET /api/videos/xyz"). Any client
// flow that calls this endpoint first to fetch metadata (title,
// mimeType, size, telegramDocumentId, etc.) before attempting playback
// would treat that 404 as "video unavailable" and render it as a
// broken/corrupted video icon even though the underlying Telegram
// message and /api/stream/:id endpoint were perfectly fine.
app.use("/api/videos", require("./routes/videos"));

// Health Check
app.get("/", (req, res) => {
    res.json({
        status: "ok",
        message: "MYFLIX MTProto Server Running",
        telegramConnected: Boolean(app.locals.telegramClient),
    });
});

const PORT = process.env.PORT || 3000;

// FIX (Telegram client not initialized on app.locals):
// Previously app.listen() ran immediately, then startClient() ran in a
// detached, un-awaited IIFE afterward. Express began accepting requests
// the instant the port opened, so any request to /api/stream/* (or
// /api/videos) that arrived before that IIFE's multi-second MTProto
// handshake finished found app.locals.telegramClient still undefined -
// producing exactly "Telegram client not initialized on app.locals" on a
// server that was otherwise perfectly healthy, just not done connecting
// yet.
//
// Awaiting startClient() before app.listen() is ever called guarantees
// app.locals.telegramClient is deterministically either set (success) or
// left unset with the real error already logged (failure) by the time the
// server accepts its first connection - the race is gone, not narrowed.
// There is still only one startClient() call in the whole app (this one),
// so there is exactly one shared Telegram client instance; routes/stream.js
// and routes/videos.js both read it from the same app.locals slot.
async function bootstrap() {
    try {
        const client = await startClient();
        app.locals.telegramClient = client;
        console.log("✅ Telegram Connected");
    } catch (err) {
        // Log the real error now, while it's still attributable to boot,
        // instead of silently leaving app.locals.telegramClient undefined
        // with no record of why. routes/stream.js already checks for a
        // missing client and returns a clean 503 rather than crashing, so
        // it's safe to keep serving non-streaming routes on failure.
        console.error("❌ Telegram Connection Error:", err);
    }

    app.listen(PORT, () => {
        console.log(`🚀 Server running on port ${PORT}`);
    });
}

bootstrap();

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
