
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
 *   7. Verify the exact number of bytes written matches Content-Length
 *      before closing the response cleanly - if it doesn't, the
 *      connection is forcibly destroyed instead of being ended normally,
 *      so the browser never mistakes a truncated body for a valid file.
 *
 * ---------------------------------------------------------------------
 * FIX (playback corruption): GramJS's client.iterDownload({ limit, ... })
 * treats `limit` as the NUMBER OF CHUNK REQUESTS to make (it mirrors
 * Telethon's iter_download 1:1), NOT a byte count. The previous version of
 * this file passed a raw byte count as `limit`, and combined with a 1 MiB
 * `requestSize`, this meant:
 *   - Telegram (depending on account tier / DC) can silently return a
 *     chunk SHORTER than the requested size even before the real end of
 *     the requested range.
 *   - GramJS's DownloadIter treats any "shorter than requestSize" chunk
 *     as end-of-stream and stops iterating entirely - even mid-range.
 *   - The old code then called res.end() unconditionally once the
 *     for-await loop finished, regardless of whether bytesSent actually
 *     reached the Content-Length that had already been promised to the
 *     browser in headers. This produced a "200/206 with a body shorter
 *     than Content-Length" response: a classically corrupt HTTP response
 *     that HTML5 <video> correctly refuses to play ("Could not play
 *     video" / "The media could not be loaded"), even though bytes were
 *     clearly being received.
 *
 * Fix: use a conservative, broadly-compatible requestSize (512 KiB),
 * pass a correctly-computed chunk COUNT as `limit` (with headroom), log
 * every short/unexpected chunk, and - critically - verify bytesSent ===
 * Content-Length before ending the response. On mismatch we destroy the
 * connection instead of completing it, so the client sees a hard failure
 * instead of a silently corrupted "successful" download.
 *
 * ---------------------------------------------------------------------
 * FIX (Cannot cast Document to any kind of InputFileLocation): passing the
 * raw Api.Document straight to iterDownload({ file: media }) depends on
 * GramJS's internal FileLike -> InputFileLocation auto-cast recognizing a
 * bare Document, which the installed version does not. We now build the
 * Api.InputDocumentFileLocation ourselves via utils/telegram.js's
 * getFileLocation() - a type every GramJS version accepts directly.
 *
 * ---------------------------------------------------------------------
 * FIX (first chunk never arrives / hangs until the browser gives up):
 * client.iterDownload() was returning a valid async iterator (the call
 * itself never threw), but the underlying RPC it issues internally never
 * resolved - no chunk, no error, just silence until the browser's own
 * timeout closed the connection ("Client connection closed before
 * response finished"). This is consistent with a hang inside GramJS's
 * higher-level chunking/iterator abstraction rather than anything wrong
 * with our offset/limit math, the InputDocumentFileLocation, or the DC
 * (fileDcId matched the client's already-connected main DC, so this was
 * never a cross-DC sender-borrowing issue).
 *
 * Fix: stop depending on iterDownload()'s internal generator entirely.
 * Download the requested byte range with a manually-driven loop of direct
 * client.invoke(new Api.upload.GetFile({ location, offset, limit }))
 * calls - the same low-level primitive that getMessage()/entity
 * resolution already use successfully in this codebase. Each individual
 * request is wrapped in its own timeout, so a single stuck RPC fails
 * fast and loud (with a clear log line) instead of hanging silently.
 * This sidesteps whatever internal bug/edge-case exists in the installed
 * iterDownload() implementation.
 *
 * Compatible with telegram@2.26.x and Render's container networking.
 */

const express = require("express");
const router = express.Router();

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia, getFileLocation } = require("../utils/telegram");
const { Api } = require("telegram");

// "big-integer" is a transitive dependency of the "telegram" package and is
// required to build correctly-typed 64-bit offsets for MTProto file downloads.
// If it is not hoisted into your top-level node_modules, add it explicitly:
//   npm install big-integer
const bigInt = require("big-integer");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

// Must be a multiple of 4096 (MTProto requirement for file offsets/limits),
// and offset must be an exact multiple of this value too (also enforced
// below). 1 MiB is technically the protocol ceiling, but several account
// tiers / DCs will silently serve less than that per request, which is
// exactly what caused truncated/corrupted playback. 512 KiB is the
// commonly-documented safe size that works reliably across regular (non-
// premium) user accounts and all DCs. Override with
// TELEGRAM_STREAM_CHUNK_SIZE (bytes) if you know your account supports more.
const MIN_CHUNK_SIZE = 4096;
const MAX_CHUNK_SIZE = 1024 * 1024; // hard MTProto ceiling for upload.getFile
const DEFAULT_CHUNK_SIZE = 512 * 1024; // 512 KiB - safe default

function resolveChunkSize() {
    const raw = Number(process.env.TELEGRAM_STREAM_CHUNK_SIZE);
    if (!Number.isFinite(raw) || raw <= 0) return DEFAULT_CHUNK_SIZE;
    const rounded = Math.floor(raw / MIN_CHUNK_SIZE) * MIN_CHUNK_SIZE;
    return Math.min(Math.max(rounded, MIN_CHUNK_SIZE), MAX_CHUNK_SIZE);
}

const CHUNK_SIZE = resolveChunkSize();

// How long we allow Telegram metadata calls (getMessage) to hang before
// treating the connection as unavailable.
const METADATA_TIMEOUT_MS = 15000;

// If no bytes have been written to the response for this long mid-stream,
// we assume the Telegram connection has stalled and abort cleanly.
const STALL_TIMEOUT_MS = 30000;

// Each individual Telegram upload.GetFile RPC gets its own timeout, so a
// single stuck request fails fast and loud instead of hanging silently
// until the browser gives up and closes the connection.
const CHUNK_REQUEST_TIMEOUT_MS = 20000;

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

req.on("aborted", () => {
    aborted = true;
    console.log(`[stream:${rid}] Client aborted request`);
});

res.on("error", (err) => {
    aborted = true;
    console.error(`[stream:${rid}] Response error:`, err.message);
});

res.on("finish", () => {
    aborted = true;
});

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
        log(rid, "info", "Loading Firestore document...", { id });

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

        log(rid, "info", "Telegram document size", { id, totalSize });

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
        log(rid, "info", "Range header", { id, range: req.headers.range || "(none - full file)" });

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

        log(rid, "info", "Actual range", { id, start, end, contentLength });

        // ---------------------------------------------------------------
        // 7 & 8. Set response headers
        // ---------------------------------------------------------------
        res.set({
            "Accept-Ranges": "bytes",
            "Content-Type": mimeType,
            "Content-Length": String(contentLength),
            "Cache-Control": "no-cache",
        });

        log(rid, "info", "Content-Length", { id, contentLength });

        if (range) {
            res.status(206);
            const contentRangeValue = `bytes ${start}-${end}/${totalSize}`;
            res.set("Content-Range", contentRangeValue);
            log(rid, "info", "Content-Range", { id, contentRange: contentRangeValue });
        } else {
            res.status(200);
            log(rid, "info", "Content-Range", { id, contentRange: "(none - 200 full file response)" });
        }

        log(rid, "info", "Starting stream", {
            id,
            title: video.title,
            season: video.season,
            episode: video.episode,
            start,
            end,
            totalSize,
            contentLength,
            chunkSize: CHUNK_SIZE,
            partial: !!range,
        });

        // ---------------------------------------------------------------
        // 9-11. Stream from Telegram in chunks (no full-file buffering)
        // ---------------------------------------------------------------
        // MTProto file offsets/limits must be aligned to the chosen chunk
        // size (offset must be an exact multiple of the requestSize used),
        // so we download from the aligned start and trim the leading bytes
        // of the first chunk to land exactly on the requested `start`.
        const alignedStart = start - (start % CHUNK_SIZE);
        const skipBytes = start - alignedStart;
        const downloadWindowBytes = end - alignedStart + 1;

        // Total bytes we must pull from Telegram to cover the requested
        // (aligned) window - used only for short-chunk/EOF sanity logging
        // in the manual download loop below.
        // (chunkLimit is no longer needed: we drive the download with an
        // explicit while-loop of individual GetFile calls rather than
        // handing a chunk-count budget to iterDownload().)

        // Build a real Api.InputDocumentFileLocation ourselves rather than
        // handing the raw Api.Document to iterDownload and relying on
        // GramJS's internal auto-cast (see getFileLocation() in
        // utils/telegram.js for why: that cast is what previously threw
        // "Cannot cast Document to any kind of InputFileLocation"). dcId is
        // not part of InputDocumentFileLocation's schema, so it must be
        // passed to iterDownload separately - this is exactly what the
        // auto-cast would have extracted from the Document for us.
        let fileLocation;
        try {
            fileLocation = getFileLocation(media);
        } catch (locErr) {
            log(rid, "error", "Failed to build InputDocumentFileLocation", {
                id,
                message: locErr.message,
            });
            return res.status(500).json({ error: "Unable to prepare video for streaming" });
        }

        log(rid, "info", "Prepared file location", {
            id,
            docId: media.id?.toString?.() ?? String(media.id),
            dcId: media.dcId,
        });

        let bytesSent = 0;
        let rawBytesReceived = 0;
        let isFirstChunk = true;
        let lastActivity = Date.now();
        let chunkIndex = 0;
        let abortReason = null; // "client_disconnect" | "stall_timeout" | null

        const stallGuard = setInterval(() => {
            if (aborted) return;
            if (Date.now() - lastActivity > STALL_TIMEOUT_MS) {
                log(rid, "error", "Stream stalled, aborting", { id, bytesSent, chunkIndex });
                abortReason = "stall_timeout";
                aborted = true;
            } else {
                log(rid, "info", "Stream progress", {
                    id,
                    chunkIndex,
                    bytesSent,
                    contentLength,
                    percent: Math.round((bytesSent / contentLength) * 100),
                });
            }
        }, 5000);

        try {
            let offset = bigInt(alignedStart);

            // Manually drive the download: request a chunk, write it,
            // advance the offset, repeat - fully explicit, no dependency
            // on iterDownload()'s internal generator/EOF heuristics.
            console.log(`[stream:${rid}] Entering download loop`, {
    aborted,
    bytesSent,
    contentLength,
    chunkIndex
});

            while (!aborted && bytesSent < contentLength) {
                chunkIndex += 1;

                let result;
                try {
                    result = await withTimeout(
                        client.invoke(
                            new Api.upload.GetFile({
                                location: fileLocation,
                                offset,
                                limit: CHUNK_SIZE,
                            })
                        ),
                        CHUNK_REQUEST_TIMEOUT_MS,
                        `Telegram GetFile chunk #${chunkIndex} (offset=${offset.toString()})`
                    );
                } catch (chunkErr) {
                    log(rid, "error", "Telegram GetFile request failed or timed out", {
                        id,
                        chunkIndex,
                        offset: offset.toString(),
                        message: chunkErr.message,
                    });
                    throw chunkErr;
                }

                if (result.className === "upload.FileCdnRedirect") {
                    log(rid, "error", "Telegram returned a CDN redirect, which is not supported", {
                        id,
                        chunkIndex,
                    });
                    throw new Error("TELEGRAM_CDN_REDIRECT_UNSUPPORTED");
                }

                const bytes = result.bytes;

                if (!bytes || bytes.length === 0) {
                    // Genuine end of file - Telegram has nothing more to give us.
                    log(rid, "warn", "Telegram returned an empty chunk (end of file)", {
                        id,
                        chunkIndex,
                        rawBytesReceived,
                        downloadWindowBytes,
                    });
                    break;
                }

                rawBytesReceived += bytes.length;
                lastActivity = Date.now();

                if (isFirstChunk) {
                    log(rid, "info", "First chunk received from Telegram", {
                        id,
                        chunkLength: bytes.length,
                        firstBytesHex: bytes.subarray(0, 16).toString("hex"),
                    });
                }

                let piece = bytes;

                if (isFirstChunk) {
                    isFirstChunk = false;
                    if (skipBytes > 0) {
                        piece = piece.subarray(skipBytes);
                    }

                    // Sanity-check that a request starting at byte 0 of the
                    // file actually looks like the start of an MP4 (an
                    // "ftyp" box at bytes 4-7). This directly verifies we
                    // are hand a valid MP4 from byte 0, not misaligned/
                    // corrupted data.
                    if (start === 0 && piece.length >= 8) {
                        const boxType = piece.subarray(4, 8).toString("ascii");
                        log(rid, "info", "First bytes written", {
                            id,
                            firstBytesHex: piece.subarray(0, 16).toString("hex"),
                            mp4BoxType: boxType,
                        });
                        if (boxType !== "ftyp" && boxType !== "moov" && boxType !== "free") {
                            log(rid, "warn", "First bytes do not look like a valid MP4 box header - possible offset/alignment issue", {
                                id,
                                boxType,
                            });
                        }
                    } else {
                        log(rid, "info", "First bytes written", {
                            id,
                            firstBytesHex: piece.subarray(0, Math.min(16, piece.length)).toString("hex"),
                        });
                    }
                }

                if (bytes.length < CHUNK_SIZE && rawBytesReceived < downloadWindowBytes) {
                    log(rid, "warn", "Received a short chunk from Telegram before expected end of range", {
                        id,
                        chunkIndex,
                        chunkLength: bytes.length,
                        requestSize: CHUNK_SIZE,
                        rawBytesReceived,
                        downloadWindowBytes,
                    });
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

                offset = offset.add(bytes.length);

                // Telegram returning fewer bytes than we asked for means
                // we've reached the real end of the file.
                if (bytes.length < CHUNK_SIZE) {
                    break;
                }
            }
        } finally {
            clearInterval(stallGuard);
        }

        log(rid, "info", "Total bytes sent", { id, bytesSent, contentLength, chunksRequested: chunkIndex });

        if (aborted) {
            if (!abortReason) abortReason = "client_disconnect";

            log(rid, "warn", "Stream aborted before completion", {
                id,
                abortReason,
                bytesSent,
                contentLength,
                durationMs: Date.now() - startedAt,
            });

            if (abortReason === "client_disconnect") {
                // The client is already gone - nothing more to send either way.
                if (!res.writableEnded) res.end();
            } else {
                // Server-initiated abort (e.g. stall timeout) while the
                // client is still connected and waiting: res.end() here
                // would silently deliver a body shorter than the
                // Content-Length we already promised, which is exactly
                // the kind of "looks complete but isn't" response that
                // breaks HTML5 video playback. Destroy the connection so
                // the browser sees a hard failure instead.
                if (!res.writableEnded) {
                    res.destroy(new Error(`Stream aborted server-side: ${abortReason}`));
                }
            }
            return;
        }

        // ---------------------------------------------------------------
        // 12. Integrity check: never let a short/corrupted body look like
        // a successful response. If we didn't write exactly the number of
        // bytes we promised in Content-Length, destroy the connection
        // instead of calling res.end() normally.
        // ---------------------------------------------------------------
        if (bytesSent !== contentLength) {
            log(rid, "error", "Stream integrity check failed - byte count mismatch, aborting connection", {
                id,
                requestedRange: req.headers.range || "(full file)",
                actualRangeServed: `${start}-${end}`,
                contentLength,
                bytesWritten: bytesSent,
                telegramDocumentSize: totalSize,
            });

            const integrityErr = new Error(
                `Stream byte mismatch for ${id}: wrote ${bytesSent} bytes, expected ${contentLength} ` +
                    `(document size ${totalSize})`
            );

            if (!res.writableEnded) {
                res.destroy(integrityErr);
            }
            return;
        }

        res.end();

        log(rid, "info", "Stream completion", {
            id,
            requestedRange: req.headers.range || "(full file)",
            actualRangeServed: `${start}-${end}`,
            contentLength,
            bytesWritten: bytesSent,
            telegramDocumentSize: totalSize,
            durationMs: Date.now() - startedAt,
        });
    } catch (err) {
        log(rid, "error", "Unhandled error in stream route", {
            id,
            message: err.message,
            stack: err.stack,
        });

        if (!res.headersSent) {
            res.status(500).json({ error: "Failed to stream video" });
        } else if (!res.writableEnded) {
            // Headers (and a Content-Length promise) were already sent -
            // ending normally here would produce a response that looks
            // complete but isn't. Destroy the connection so the client
            // sees a hard failure instead of a silently corrupted file.
            res.destroy(err);
        }
    } finally {
    req.removeAllListeners("aborted");
    res.removeAllListeners("error");
    res.removeAllListeners("finish");
}
});

module.exports = router;
