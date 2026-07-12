const express = require("express");
const router = express.Router();

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia, getFileLocation } = require("../utils/telegram");
const { Api } = require("telegram");

const bigInt = require("big-integer");

const MIN_CHUNK_SIZE = 4096;
const MAX_CHUNK_SIZE = 1024 * 1024;
const DEFAULT_CHUNK_SIZE = 512 * 1024;

function resolveChunkSize() {
    const raw = Number(process.env.TELEGRAM_STREAM_CHUNK_SIZE);
    if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_CHUNK_SIZE;
    const rounded = Math.floor(raw / MIN_CHUNK_SIZE) * MIN_CHUNK_SIZE;
    return Math.min(Math.max(rounded, MIN_CHUNK_SIZE), MAX_CHUNK_SIZE);
}

const CHUNK_SIZE = resolveChunkSize();
const METADATA_TIMEOUT_MS = 15000;
const STALL_TIMEOUT_MS = 30000;
const CHUNK_REQUEST_TIMEOUT_MS = 25000;

const VALID_ID_REGEX = /^[a-zA-Z0-9_-]+$/;

function reqId() {
    return Math.random().toString(36).slice(2, 8);
}

function log(rid, level, msg, meta) {
    const line = `[stream:${rid}] ${msg}`;
    if (level === "error") console.error(line, meta || "");
    else if (level === "warn") console.warn(line, meta || "");
    else console.log(line, meta || "");
}

function toNumber(value) {
    if (value === null || value === undefined) return NaN;
    if (typeof value === "number") return value;
    if (typeof value === "bigint") return Number(value);
    if (typeof value.toJSNumber === "function") return value.toJSNumber();
    return Number(value.toString());
}

function withTimeout(promise, ms, label) {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    });
    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

function parseRange(rangeHeader, totalSize) {
    if (!rangeHeader) return null;
    const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader.trim());
    if (!match) throw Object.assign(new Error("Malformed Range"), {type: "RANGE_MALFORMED"});
    let start = match[1] ? parseInt(match[1], 10) : 0;
    let end = match[2] ? parseInt(match[2], 10) : totalSize - 1;
    if (Number.isNaN(start) || Number.isNaN(end) || start > end || start >= totalSize) {
        throw Object.assign(new Error("Range not satisfiable"), {type: "RANGE_NOT_SATISFIABLE"});
    }
    if (end >= totalSize) end = totalSize - 1;
    return { start, end };
}

router.get("/:id", async (req, res) => {
    const rid = reqId();
    const startedAt = Date.now();
    const { id } = req.params;

    let aborted = false;
    let abortReason = null;

    const onClose = () => {
        if (aborted) return;
        if (!res.writableEnded) {
            aborted = true;
            abortReason = "client_disconnect";
            log(rid, "warn", "Client disconnected", { id });
        }
    };
    res.on("close", onClose);
    req.on("aborted", onClose);

    try {
        if (!VALID_ID_REGEX.test(id)) return res.status(400).json({ error: "Invalid id" });

        const client = req.app.locals.telegramClient;
        if (!client) return res.status(503).json({ error: "Service unavailable" });

        const doc = await db.collection("videos").doc(id).get();
        if (!doc.exists) return res.status(404).json({ error: "Video not found" });

        const video = doc.data();
        let message = await withTimeout(getMessage(client, video.channelId, video.messageId), METADATA_TIMEOUT_MS, "getMessage");
        if (!message) return res.status(404).json({ error: "Message not found" });

        const media = getVideoMedia(message);
        if (!media) return res.status(404).json({ error: "No media" });

        const totalSize = toNumber(media.size);
        if (!Number.isFinite(totalSize) || totalSize <= 0) return res.status(500).json({ error: "Invalid size" });

        const mimeType = media.mimeType?.startsWith("video/") ? media.mimeType : "video/mp4";

        let range;
        try {
            range = parseRange(req.headers.range, totalSize);
        } catch (e) {
            if (e.type === "RANGE_NOT_SATISFIABLE") {
                res.set("Content-Range", `bytes */${totalSize}`);
                return res.status(416).end();
            }
            return res.status(400).json({ error: "Bad range" });
        }

        const start = range ? range.start : 0;
        const end = range ? range.end : totalSize - 1;
        const contentLength = end - start + 1;

        res.set({
            "Accept-Ranges": "bytes",
            "Content-Type": mimeType,
            "Content-Length": contentLength.toString(),
            "Cache-Control": "no-cache",
        });

        if (range) {
            res.status(206);
            res.set("Content-Range", `bytes ${start}-${end}/${totalSize}`);
        } else {
            res.status(200);
        }

        // CRITICAL FIXES
        res.flushHeaders();
        log(rid, "info", "Headers flushed", { id, start, contentLength });

        // Send a tiny dummy byte immediately to keep connection alive
        if (contentLength > 0) {
            res.write(new Uint8Array([0x00]));
        }

        let fileLocation = getFileLocation(media);
        let bytesSent = contentLength > 0 ? 1 : 0;
        let offset = bigInt(start);
        let chunkIndex = 0;
        let lastActivity = Date.now();

        const stallGuard = setInterval(() => {
            if (Date.now() - lastActivity > STALL_TIMEOUT_MS) {
                aborted = true;
                abortReason = "stall";
            }
        }, 5000);

        try {
            while (!aborted && bytesSent < contentLength) {
                chunkIndex++;
                const result = await withTimeout(
                    client.invoke(new Api.upload.GetFile({ location: fileLocation, offset, limit: CHUNK_SIZE })),
                    CHUNK_REQUEST_TIMEOUT_MS,
                    `GetFile #${chunkIndex}`
                );

                const bytes = result.bytes || Buffer.alloc(0);
                if (bytes.length === 0) break;

                lastActivity = Date.now();
                let piece = bytes;

                const remaining = contentLength - bytesSent;
                if (piece.length > remaining) piece = piece.subarray(0, remaining);

                if (piece.length > 0) {
                    res.write(piece);
                    bytesSent += piece.length;
                }

                offset = offset.add(bytes.length);
                if (bytes.length < CHUNK_SIZE) break;
            }
        } finally {
            clearInterval(stallGuard);
        }

        if (!aborted && bytesSent >= contentLength - 1) {
            res.end();
            log(rid, "info", "Stream completed", { id, bytesSent });
        } else if (!res.writableEnded) {
            res.destroy();
        }

    } catch (err) {
        log(rid, "error", "Stream error", { message: err.message });
        if (!res.headersSent) res.status(500).json({ error: "Failed" });
        else if (!res.writableEnded) res.destroy(err);
    }
});

module.exports = router;
