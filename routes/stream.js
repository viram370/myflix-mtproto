const express = require("express");
const router = express.Router();

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia, getFileLocation } = require("../utils/telegram");
const { getClientForDC } = require("../gramjs");
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
const CHUNK_REQUEST_TIMEOUT_MS = 20000;
const MAX_DC_MIGRATION_HOPS = 3;
const VALID_ID_REGEX = /^[a-zA-Z0-9_-]+$/;

function parseMigrateError(err) {
    const msg = (err && (err.errorMessage || err.message)) || "";
    const match = /^(FILE|NETWORK)_MIGRATE_(\d+)$/.exec(String(msg).trim());
    if (match) return { kind: match[1], dcId: parseInt(match[2], 10) };
    
    // Fallback extraction to guarantee we never leak or throw the raw string
    const stringMatch = /currently stored in DC (\d+)/i.exec(String(msg));
    if (stringMatch) return { kind: 'FILE', dcId: parseInt(stringMatch[1], 10) };
    
    return null;
}

function reqId() {
    return Math.random().toString(36).slice(2, 8);
}

function log(rid, level, msg, meta) {
    const line = `[stream:${rid}] ${msg}`;
    const payload = meta ? { ...meta } : undefined;
    if (level === "error") console.error(line, payload || "");
    else if (level === "warn") console.warn(line, payload || "");
    else console.log(line, payload || "");
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
    if (!match || (match[1] === "" && match[2] === "")) {
        const err = new Error("Malformed Range header");
        err.type = "RANGE_MALFORMED";
        throw err;
    }
    let start, end;
    if (match[1] === "") {
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
    if (Number.isNaN(start) || Number.isNaN(end) || start < 0 || start > end || start >= totalSize) {
        const err = new Error("Requested range not satisfiable");
        err.type = "RANGE_NOT_SATISFIABLE";
        throw err;
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
            abortReason = abortReason || "client_disconnect";
            log(rid, "warn", "Client connection closed before response finished", {
                id,
                reqAborted: req.aborted,
                resWritableEnded: res.writableEnded,
                elapsedMs: Date.now() - startedAt,
            });
        }
    };
    res.on("close", onClose);

    const onReqAborted = () => {
        if (aborted) return;
        if (!res.writableEnded) {
            aborted = true;
            abortReason = abortReason || "client_disconnect";
            log(rid, "warn", "Request aborted by client", { id });
        }
    };
    req.on("aborted", onReqAborted);

    try {
        if (!id || typeof id !== "string" || !VALID_ID_REGEX.test(id)) {
            log(rid, "warn", "Invalid video id", { id });
            return res.status(400).json({ error: "Invalid video id" });
        }

        const client = req.app.locals.telegramClient;
        if (!client) {
            log(rid, "error", "Telegram client not initialized");
            return res.status(503).json({ error: "Streaming service unavailable" });
        }
        if (!client.connected) {
            try {
                await withTimeout(client.connect(), METADATA_TIMEOUT_MS, "Telegram connect");
            } catch (connectErr) {
                log(rid, "error", "Telegram connect failed", { message: connectErr.message });
                return res.status(503).json({ error: "Telegram unavailable" });
            }
        }

        log(rid, "info", "Loading Firestore document...", { id });
        let doc;
        try {
            doc = await db.collection("videos").doc(id).get();
        } catch (dbErr) {
            log(rid, "error", "Firestore error", { id, message: dbErr.message });
            return res.status(500).json({ error: "Failed to load metadata" });
        }

        if (!doc.exists) return res.status(404).json({ error: "Video not found" });

        const video = doc.data();
        if (!video?.channelId || !video?.messageId) {
            return res.status(404).json({ error: "Incomplete video metadata" });
        }

        let message;
        try {
            message = await withTimeout(
                getMessage(client, video.channelId, video.messageId),
                METADATA_TIMEOUT_MS,
                "Telegram getMessage"
            );
        } catch (tgErr) {
            log(rid, "error", "getMessage failed", { id, message: tgErr.message });
            return res.status(503).json({ error: "Telegram unavailable" });
        }

        if (!message) return res.status(404).json({ error: "Source message not found" });

        const media = getVideoMedia(message);
        if (!media) return res.status(404).json({ error: "Video media not found" });

        const totalSize = toNumber(media.size);
        if (!Number.isFinite(totalSize) || totalSize <= 0) {
            return res.status(500).json({ error: "Invalid media size" });
        }

        const mimeType = media.mimeType?.startsWith("video/") ? media.mimeType : "video/mp4";

        let range;
        try {
            range = parseRange(req.headers.range, totalSize);
        } catch (rangeErr) {
            if (rangeErr.type === "RANGE_NOT_SATISFIABLE") {
                res.set("Content-Range", `bytes */${totalSize}`);
                return res.status(416).json({ error: "Range not satisfiable" });
            }
            return res.status(400).json({ error: "Malformed Range" });
        }

        const start = range ? range.start : 0;
        const end = range ? range.end : totalSize - 1;
        const contentLength = end - start + 1;

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

        res.flushHeaders();
        log(rid, "info", "Headers flushed to client - starting stream", {
            id, start, end, contentLength, chunkSize: CHUNK_SIZE,
        });

        const alignedStart = start - (start % CHUNK_SIZE);
        const skipBytes = start - alignedStart;

        let fileLocation;
        try {
            fileLocation = getFileLocation(media);
        } catch (locErr) {
            log(rid, "error", "Failed to build file location", { id, message: locErr.message });
            return res.status(500).json({ error: "Unable to prepare video" });
        }

        let bytesSent = 0;
        let rawBytesReceived = 0;
        let isFirstChunk = true;
        let lastActivity = Date.now();
        let chunkIndex = 0;

        const stallGuard = setInterval(() => {
            if (aborted) return;
            if (Date.now() - lastActivity > STALL_TIMEOUT_MS) {
                log(rid, "error", "Stream stalled", { id, bytesSent });
                aborted = true;
                abortReason = "stall_timeout";
            }
        }, 5000);

        log(rid, "info", "Entering download loop", { id, aborted });

        let activeClient = client;
        let activeDcId = client.session.dcId;
        const fileDcId = media.dcId !== undefined ? media.dcId : activeDcId;

        console.log(`[MTProto] Home DC: ${client.session.dcId}`);
        console.log(`[MTProto] Current Client DC: ${activeDcId}`);
        console.log(`[MTProto] File DC: ${fileDcId}`);

        if (fileDcId !== activeDcId) {
            try {
                activeClient = await getClientForDC(fileDcId);
                activeDcId = fileDcId;
            } catch (migrationErr) {
                log(rid, "error", "DC migration failed pre-emptively", { id, targetDc: fileDcId, message: migrationErr.message });
                return res.status(500).json({ error: "DC migration failed" });
            }
        }

        console.log("[MTProto] Starting download...");

        try {
            let offset = bigInt(alignedStart);
            while (!aborted && bytesSent < contentLength) {
                chunkIndex += 1;
                let result;

                for (let hop = 0; hop <= MAX_DC_MIGRATION_HOPS; hop++) {
                    try {
                        result = await withTimeout(
                            activeClient.invoke(new Api.upload.GetFile({ location: fileLocation, offset, limit: CHUNK_SIZE })),
                            CHUNK_REQUEST_TIMEOUT_MS,
                            `GetFile chunk #${chunkIndex}`
                        );
                        break; 
                    } catch (chunkErr) {
                        const migrate = parseMigrateError(chunkErr);

                        if (!migrate) {
                            log(rid, "error", "GetFile failed", { id, chunkIndex, offset: offset.toString(), message: chunkErr.message });
                            throw chunkErr;
                        }

                        if (hop === MAX_DC_MIGRATION_HOPS) {
                            log(rid, "error", "DC migration exceeded max hops - giving up", {
                                id, chunkIndex, offset: offset.toString(), targetDc: migrate.dcId, hops: hop,
                            });
                            throw new Error(`Migration exceeded max hops for DC ${migrate.dcId}`);
                        }

                        if (hop === 0 && fileDcId === activeDcId) {
                            console.log(`[MTProto] File DC: ${migrate.dcId}`);
                        }

                        log(rid, "warn", `${migrate.kind}_MIGRATE_${migrate.dcId} detected - switching client DC`, {
                            id, chunkIndex, currentDc: activeDcId, targetDc: migrate.dcId, offset: offset.toString(), hop: hop + 1,
                        });

                        try {
                            activeClient = await getClientForDC(migrate.dcId);
                            activeDcId = migrate.dcId;
                        } catch (migrationErr) {
                            log(rid, "error", "DC migration failed", { id, targetDc: migrate.dcId, message: migrationErr.message });
                            throw migrationErr; 
                        }

                        log(rid, "info", "Resumed streaming on new DC", { id, chunkIndex, dc: activeDcId, offset: offset.toString() });
                    }
                }

                if (result.className === "upload.FileCdnRedirect") throw new Error("CDN redirect unsupported");

                const bytes = result.bytes;
                if (!bytes || bytes.length === 0) break;

                rawBytesReceived += bytes.length;
                lastActivity = Date.now();

                let piece = bytes;
                if (isFirstChunk) {
                    isFirstChunk = false;
                    if (skipBytes > 0) piece = piece.subarray(skipBytes);
                }

                const remaining = contentLength - bytesSent;
                if (piece.length > remaining) piece = piece.subarray(0, remaining);

                if (piece.length > 0) {
                    const canContinue = res.write(piece);
                    bytesSent += piece.length;
                    lastActivity = Date.now();
                    if (!canContinue) await new Promise(resolve => res.once("drain", resolve));
                }

                offset = offset.add(bytes.length);
                if (bytes.length < CHUNK_SIZE) break;
            }
        } finally {
            clearInterval(stallGuard);
        }

        log(rid, "info", "Stream finished", { id, bytesSent, contentLength });

        if (aborted) {
            if (!res.writableEnded) res.destroy(new Error(`Aborted: ${abortReason}`));
            return;
        }

        if (bytesSent !== contentLength) {
            log(rid, "error", "Integrity failure", { id, bytesSent, contentLength });
            if (!res.writableEnded) res.destroy(new Error("Byte count mismatch"));
            return;
        }

        res.end();
        console.log("[MTProto] Download completed.");
        log(rid, "info", "Stream completed successfully", { id });

    } catch (err) {
        const migrate = parseMigrateError(err);
        if (migrate) {
            log(rid, "error", "DC migration could not be completed", { id, targetDc: migrate.dcId, message: err.message });
        } else {
            const msg = err.message || "";
            if (msg.includes("currently stored in DC")) {
                log(rid, "error", "DC migration could not be completed", { id, message: msg });
            } else {
                log(rid, "error", "Unhandled stream error", { id, message: msg });
            }
        }
        if (!res.headersSent) res.status(500).json({ error: "Stream failed" });
        else if (!res.writableEnded) res.destroy(err);
    } finally {
        res.off("close", onClose);
        req.off("aborted", onReqAborted);
    }
});

module.exports = router;
