
/**
 * routes/stream.js
 *
 * Streams video files directly from Telegram (via MTProto / GramJS)
 * to the browser, with full HTTP Range support for HTML5 <video> seeking.
 *
 * Flow:
 *   1. Validate :id
 *   2. Load video document from Firestore ("videos" collection)
 *   3. Resolve the Telegram message (channelId + messageId)
 *   4. Resolve the video media (document) attached to that message
 *   5. Parse the Range header and compute byte window
 *   6. Stream the requested byte range from Telegram using
 *      client.iterDownload(), writing chunks straight to the response
 *      (no buffering of the whole file in memory / disk)
 *
 * Compatible with telegram@2.26.x and Render's container networking.
 */

const express = require("express");
const router = express.Router();

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia } = require("../utils/telegram");

// "big-integer" is a transitive dependency of the "telegram" package and is
// required to build correctly-typed 64-bit offsets for MTProto file downloads.
// If it is not hoisted into your top-level node_modules, add it explicitly:
//   npm install big-integer
const bigInt = require("big-integer");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Must be a multiple of 4096 (MTProto requirement for file offsets/limits).
// 1 MiB is a safe, efficient default for both small clips and 2GB+ movies.
const CHUNK_SIZE = 1024 * 1024; // 1 MiB

// How long we allow Telegram metadata calls (getMessage) to hang before
// treating the connection as unavailable.
const METADATA_TIMEOUT_MS = 15000;

// If no bytes have been written to the response for this long mid-stream,
// we assume the Telegram connection has stalled and abort cleanly.
const STALL_TIMEOUT_MS = 30000;

const VALID_ID_REGEX = /^[a-zA-Z0-9_-]+$/;

// ---------------------------------------------------------------------------
// Logging helpers
// ---------------------------------------------------------------------------

function reqId() {
    return Math.random().toString(36).slice(2, 8);
}

function log(rid, level, msg, meta) {
    const line = `[stream:${rid}] ${msg}`;
    const payload = meta ? { ...meta } : undefined;

    if (level === "error") {
        console.error(line, payload || "");
    } else if (level === "warn") {
        console.warn(line, payload || "");
    } else {
        console.log(line, payload || "");
    }
}

// ---------------------------------------------------------------------------
// Small utilities
// ---------------------------------------------------------------------------

/**
 * Safely convert a Number / BigInt / big-integer instance / numeric string
 * into a JS Number. Video sizes fit well within Number.MAX_SAFE_INTEGER
 * (up to ~9 petabytes), so this is safe for arithmetic on file sizes/offsets.
 */
function toNumber(value) {
    if (value === null || value === undefined) return NaN;
    if (typeof value === "number") return value;
    if (typeof value === "bigint") return Number(value);
    if (typeof value.toJSNumber === "function") return value.toJSNumber();
    return Number(value.toString());
}

/**
 * Wrap a promise with a timeout so a hung Telegram call can't hang the
 * whole request/connection indefinitely.
 */
function withTimeout(promise, ms, label) {
    let timer;
    const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => {
            reject(new Error(`${label} timed out after ${ms}ms`));
        }, ms);
    });

    return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Parse a "bytes=start-end" Range header against a known total size.
 * Returns { start, end } or null if there was no range header (full file),
 * or throws a typed error for unsatisfiable / malformed ranges.
 */
function parseRange(rangeHeader, totalSize) {
    if (!rangeHeader) return null;

    const match = /^bytes=(\d*)-(\d*)$/.exec(rangeHeader.trim());

    if (!match || (match[1] === "" && match[2] === "")) {
        const err = new Error("Malformed Range header");
        err.type = "RANGE_MALFORMED";
        throw err;
    }

    let start;
    let end;

    if (match[1] === "") {
        // Suffix range: "bytes=-500" => last 500 bytes
        const suffixLength = parseInt(match[2], 10);
        if (Number.isNaN(suffixLength) || suffixLength <= 0) {
            const err = new Error("Malformed suffix range");
            err.type = "RANGE_MALFORMED";
            throw err;
        }
        start = Math.max(totalSize - suffixLength, 0);
        end = totalSize - 1;
    } else {
        start = parseInt(match[1], 10);
        end = match[2] === "" ? totalSize - 1 : parseInt(match[2], 10);
    }

    if (
        Number.isNaN(start) ||
        Number.isNaN(end) ||
        start < 0 ||
        start > end ||
        start >= totalSize
    ) {
        const err = new Error("Requested range not satisfiable");
        err.type = "RANGE_NOT_SATISFIABLE";
        throw err;
    }

    if (end >= totalSize) {
        end = totalSize - 1;
    }

    return { start, end };
}

// ---------------------------------------------------------------------------
// Route
// ---------------------------------------------------------------------------

router.get("/:id", async (req, res) => {
    const rid = reqId();
    const startedAt = Date.now();
    const { id } = req.params;

    let aborted = false;
    let downloadIterator = null;

    // If the client disconnects mid-stream (closes tab, seeks again, etc.)
    // stop pulling bytes from Telegram immediately.
    const onClose = () => {
        aborted = true;
        if (downloadIterator && typeof downloadIterator.return === "function") {
            downloadIterator.return().catch(() => {});
        }
    };
    res.on("close", onClose);

    try {
        // ---------------------------------------------------------------
        // 1. Validate :id
        // ---------------------------------------------------------------
        if (!id || typeof id !== "string" || !VALID_ID_REGEX.test(id)) {
            log(rid, "warn", "Invalid video id", { id });
            return res.status(400).json({ error: "Invalid video id" });
        }

        // ---------------------------------------------------------------
        // 2. Telegram client availability
        // ---------------------------------------------------------------
        const client = req.app.locals.telegramClient;

        if (!client) {
            log(rid, "error", "Telegram client not initialized on app.locals");
            return res.status(503).json({ error: "Streaming service unavailable" });
        }

        if (!client.connected) {
            try {
                await withTimeout(client.connect(), METADATA_TIMEOUT_MS, "Telegram connect");
            } catch (connectErr) {
                log(rid, "error", "Telegram client failed to connect", {
                    message: connectErr.message,
                });
                return res.status(503).json({ error: "Telegram is currently unavailable" });
            }
        }

        // ---------------------------------------------------------------
        // 3. Load Firestore document
        // ---------------------------------------------------------------
        let doc;
        try {
            doc = await db.collection("videos").doc(id).get();
        } catch (dbErr) {
            log(rid, "error", "Firestore error while fetching video document", {
                id,
                message: dbErr.message,
            });
            return res.status(500).json({ error: "Failed to load video metadata" });
        }

        if (!doc.exists) {
            log(rid, "warn", "Video document not found", { id });
            return res.status(404).json({ error: "Video not found" });
        }

        const video = doc.data();

        if (!video || !video.channelId || !video.messageId) {
            log(rid, "warn", "Video document missing channelId/messageId", { id });
            return res.status(404).json({ error: "Video source metadata is incomplete" });
        }

        // ---------------------------------------------------------------
        // 4. Resolve Telegram message
        // ---------------------------------------------------------------
        let message;
        try {
            message = await withTimeout(
                getMessage(client, video.channelId, video.messageId),
                METADATA_TIMEOUT_MS,
                "Telegram getMessage"
            );
        } catch (tgErr) {
            log(rid, "error", "Telegram getMessage failed", {
                id,
                message: tgErr.message,
            });
            return res.status(503).json({ error: "Telegram is currently unavailable" });
        }

        if (!message) {
            log(rid, "warn", "Telegram message not found", {
                id,
                channelId: video.channelId,
                messageId: video.messageId,
            });
            return res.status(404).json({ error: "Source message not found" });
        }

        // ---------------------------------------------------------------
        // 5. Resolve video media
        // ---------------------------------------------------------------
        const media = getVideoMedia(message);

        if (!media) {
            log(rid, "warn", "No video media on message", { id });
            return res.status(404).json({ error: "Video media not found" });
        }

        const totalSize = toNumber(media.size);

        if (!Number.isFinite(totalSize) || totalSize <= 0) {
            log(rid, "error", "Unable to determine media size", { id });
            return res.status(500).json({ error: "Unable to determine video size" });
        }

        const mimeType =
            media.mimeType && typeof media.mimeType === "string"
                ? media.mimeType
                : "video/mp4";

        // ---------------------------------------------------------------
        // 6. Parse Range header
        // ---------------------------------------------------------------
        let range;
        try {
            range = parseRange(req.headers.range, totalSize);
        } catch (rangeErr) {
            if (rangeErr.type === "RANGE_NOT_SATISFIABLE") {
                log(rid, "warn", "Range not satisfiable", {
                    id,
                    range: req.headers.range,
                    totalSize,
                });
                res.set("Content-Range", `bytes */${totalSize}`);
                return res.status(416).json({ error: "Requested range not satisfiable" });
            }

            log(rid, "warn", "Malformed range header", {
                id,
                range: req.headers.range,
            });
            return res.status(400).json({ error: "Malformed Range header" });
        }

        const start = range ? range.start : 0;
        const end = range ? range.end : totalSize - 1;
        const contentLength = end - start + 1;

        // ---------------------------------------------------------------
        // 7 & 8. Set response headers
        // ---------------------------------------------------------------
        res.set({
            "Accept-Ranges": "bytes",
            "Content-Type": mimeType,
            "Content-Length": String(contentLength),
            "Cache-Control": "no-cache",
        });

        if (range) {
            res.status(206);
            res.set("Content-Range", `bytes ${start}-${end}/${totalSize}`);
        } else {
            res.status(200);
        }

        log(rid, "info", "Starting stream", {
            id,
            title: video.title,
            season: video.season,
            episode: video.episode,
            start,
            end,
            totalSize,
            partial: !!range,
        });

        // ---------------------------------------------------------------
        // 9-11. Stream from Telegram in chunks (no full-file buffering)
        // ---------------------------------------------------------------
        // MTProto file offsets/limits must be aligned to 4096-byte boundaries,
        // so we download from the aligned start and trim the leading bytes
        // of the first chunk to land exactly on the requested `start`.
        const alignedStart = start - (start % CHUNK_SIZE);
        const skipBytes = start - alignedStart;
        const downloadLimit = end - alignedStart + 1;

        downloadIterator = client.iterDownload({
            file: media,
            offset: bigInt(alignedStart),
            limit: downloadLimit,
            requestSize: CHUNK_SIZE,
        });

        let bytesSent = 0;
        let isFirstChunk = true;
        let lastActivity = Date.now();

        const stallGuard = setInterval(() => {
            if (aborted) return;
            if (Date.now() - lastActivity > STALL_TIMEOUT_MS) {
                log(rid, "error", "Stream stalled, aborting", { id, bytesSent });
                aborted = true;
                if (downloadIterator && typeof downloadIterator.return === "function") {
                    downloadIterator.return().catch(() => {});
                }
            }
        }, 5000);

        try {
            for await (const chunk of downloadIterator) {
                if (aborted) break;

                let piece = chunk;

                if (isFirstChunk) {
                    isFirstChunk = false;
                    if (skipBytes > 0) {
                        piece = piece.subarray(skipBytes);
                    }
                }

                const remaining = contentLength - bytesSent;
                if (piece.length > remaining) {
                    piece = piece.subarray(0, remaining);
                }

                if (piece.length > 0) {
                    const canContinue = res.write(piece);
                    bytesSent += piece.length;
                    lastActivity = Date.now();

                    if (!canContinue) {
                        await new Promise((resolve) => res.once("drain", resolve));
                    }
                }

                if (bytesSent >= contentLength) break;
            }
        } finally {
            clearInterval(stallGuard);
        }

        if (!aborted) {
            res.end();
            log(rid, "info", "Stream completed", {
                id,
                bytesSent,
                durationMs: Date.now() - startedAt,
            });
        } else {
            log(rid, "warn", "Stream aborted before completion", {
                id,
                bytesSent,
                durationMs: Date.now() - startedAt,
            });
            if (!res.writableEnded) res.end();
        }
    } catch (err) {
        log(rid, "error", "Unhandled error in stream route", {
            id,
            message: err.message,
            stack: err.stack,
        });

        if (!res.headersSent) {
            res.status(500).json({ error: "Failed to stream video" });
        } else if (!res.writableEnded) {
            res.end();
        }
    } finally {
        res.off("close", onClose);
    }
});

module.exports = router;
