
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
 * getFileLocation() - a type every GramJS version accepts directly - and
 * pass the Document's dcId explicitly via iterDownload's own `dcId` option,
 * since that field isn't part of InputDocumentFileLocation's TL schema.
 *
 * ---------------------------------------------------------------------
 * FIX (playback still fails after the byte-integrity fix, even though full
 * downloads succeed): the route only ever registered a GET handler. Express
 * automatically dispatches HEAD requests to a matching GET route when no
 * explicit HEAD handler exists (Route.prototype._handles_method falls back
 * to 'get' for 'head'). Many Android WebViews / native players embedded in
 * Telegram Mini Apps probe a video URL with a HEAD request first to read
 * Accept-Ranges/Content-Length *before* issuing the real ranged GET used
 * for actual playback. Because this route had no dedicated HEAD handler,
 * that probe fell all the way through the full metadata-resolution +
 * Telegram byte-streaming pipeline - i.e. a HEAD request silently
 * triggered a full MTProto download attempt whose body Node then discards
 * (HEAD responses never send a body). This is slow, wastes a full
 * download's worth of Telegram round-trips, and can hit STALL_TIMEOUT_MS /
 * hang the probe entirely - so the player's HEAD probe times out or
 * errors, and it never gets to the real GET Range request at all,
 * producing exactly the observed symptom: duration/thumbnail already known
 * via some other path, play button present, but Play does nothing /
 * immediately stops with "Could not play video. Try again later."
 *
 * A normal HTTP video server answers HEAD with just headers (Accept-Ranges,
 * Content-Type, Content-Length, Content-Range if a Range header was sent)
 * and an empty body - no bytes read from the backing store at all. This
 * file now registers a real router.head() handler that does exactly that,
 * sharing the same metadata-resolution logic as GET but never touching
 * client.iterDownload().
 *
 * Compatible with telegram@2.26.x and Render's container networking.
 */

const express = require("express");
const router = express.Router();
const crypto = require("crypto");

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia, getFileLocation } = require("../utils/telegram");

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

// How many leading bytes of the very first chunk actually written to the
// response we log (hex), so a truncated/misaligned stream shows up
// immediately in logs instead of only surfacing as a vague player error.
// 16 bytes is enough to see whether we start with a valid ISO-BMFF box
// header (e.g. "....ftyp" for MP4) or with garbage.
const FIRST_BYTES_LOG_LENGTH = 16;

// How many bytes of the actual response body (starting at the requested
// Range start) we buffer in memory to SHA-256 for diagnostics. This lets
// you verify, for any Range request, that the bytes we actually sent are
// byte-identical to the source file at that offset - e.g. by running
//   dd if=<a full download of the same video> bs=1 skip=<start> count=<n> | sha256sum
// and comparing it to the "SHA-256 of streamed bytes (sample)" log line.
// 1 MiB keeps memory bounded while still catching any misalignment, which
// always shows up in the first chunk or two.
const SHA256_SAMPLE_BYTES = 1024 * 1024;

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
// Shared metadata resolution (used by both GET and HEAD)
// ---------------------------------------------------------------------------

/**
 * Resolves :id all the way down to a usable { media, totalSize, mimeType,
 * video } bundle, or writes an appropriate error response and returns null.
 * Shared by GET (which goes on to stream bytes) and HEAD (which must
 * answer with the exact same headers a GET would send, but WITHOUT ever
 * calling client.iterDownload() / touching Telegram for actual file bytes).
 */
async function resolveStreamTarget(req, res, rid) {
    const { id } = req.params;

    if (!id || typeof id !== "string" || !VALID_ID_REGEX.test(id)) {
        log(rid, "warn", "Invalid video id", { id });
        res.status(400).json({ error: "Invalid video id" });
        return null;
    }

    const client = req.app.locals.telegramClient;

    if (!client) {
        log(rid, "error", "Telegram client not initialized on app.locals");
        res.status(503).json({ error: "Streaming service unavailable" });
        return null;
    }

    if (!client.connected) {
        try {
            await withTimeout(client.connect(), METADATA_TIMEOUT_MS, "Telegram connect");
        } catch (connectErr) {
            log(rid, "error", "Telegram client failed to connect", {
                message: connectErr.message,
            });
            res.status(503).json({ error: "Telegram is currently unavailable" });
            return null;
        }
    }

    log(rid, "info", "Loading Firestore document...", { id });

    let doc;
    try {
        doc = await db.collection("videos").doc(id).get();
    } catch (dbErr) {
        log(rid, "error", "Firestore error while fetching video document", {
            id,
            message: dbErr.message,
        });
        res.status(500).json({ error: "Failed to load video metadata" });
        return null;
    }

    if (!doc.exists) {
        log(rid, "warn", "Video document not found", { id });
        res.status(404).json({ error: "Video not found" });
        return null;
    }

    const video = doc.data();

    if (!video || !video.channelId || !video.messageId) {
        log(rid, "warn", "Video document missing channelId/messageId", { id });
        res.status(404).json({ error: "Video source metadata is incomplete" });
        return null;
    }

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
        res.status(503).json({ error: "Telegram is currently unavailable" });
        return null;
    }

    if (!message) {
        log(rid, "warn", "Telegram message not found", {
            id,
            channelId: video.channelId,
            messageId: video.messageId,
        });
        res.status(404).json({ error: "Source message not found" });
        return null;
    }

    const media = getVideoMedia(message);

    if (!media) {
        log(rid, "warn", "No video media on message", { id });
        res.status(404).json({ error: "Video media not found" });
        return null;
    }

    const totalSize = toNumber(media.size);

    log(rid, "info", "Telegram document size", { id, totalSize });

    if (!Number.isFinite(totalSize) || totalSize <= 0) {
        log(rid, "error", "Unable to determine media size", { id });
        res.status(500).json({ error: "Unable to determine video size" });
        return null;
    }

    const mimeType =
        media.mimeType && typeof media.mimeType === "string"
            ? media.mimeType
            : "video/mp4";

    return { client, video, media, totalSize, mimeType };
}

/**
 * Given a resolved target and the incoming request, parses the Range header
 * and sets every header a normal HTTP video server would send (Accept-
 * Ranges, Content-Type, Content-Length, Content-Range + 206 status when a
 * Range was requested). Returns { start, end, contentLength, range } or
 * null if an error response (416/400) was already written.
 */
function applyRangeHeaders(req, res, rid, id, totalSize, mimeType) {
    log(rid, "info", "Requested range", { id, range: req.headers.range || "(full file)" });

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
            res.status(416).json({ error: "Requested range not satisfiable" });
            return null;
        }

        log(rid, "warn", "Malformed range header", {
            id,
            range: req.headers.range,
        });
        res.status(400).json({ error: "Malformed Range header" });
        return null;
    }

    const start = range ? range.start : 0;
    const end = range ? range.end : totalSize - 1;
    const contentLength = end - start + 1;

    log(rid, "info", "Actual range", { id, start, end, contentLength });

    res.set({
        "Accept-Ranges": "bytes",
        "Content-Type": mimeType,
        "Content-Length": String(contentLength),
        "Cache-Control": "no-cache",
    });

    log(rid, "info", "Content-Length", { id, contentLength });

    if (range) {
        res.status(206);
        res.set("Content-Range", `bytes ${start}-${end}/${totalSize}`);
        log(rid, "info", "Content-Range", { id, value: `bytes ${start}-${end}/${totalSize}` });
    } else {
        res.status(200);
    }

    return { start, end, contentLength, range };
}

// ---------------------------------------------------------------------------
// HEAD /:id - headers only, identical to what the GET would send, but never
// touches Telegram for actual file bytes. This is what fixes players/
// WebViews that probe with HEAD before issuing the real ranged GET.
// ---------------------------------------------------------------------------

router.head("/:id", async (req, res) => {
    const rid = reqId();
    const { id } = req.params;

    try {
        const target = await resolveStreamTarget(req, res, rid);
        if (!target) return; // error response already written

        const parsed = applyRangeHeaders(req, res, rid, id, target.totalSize, target.mimeType);
        if (!parsed) return; // 416/400 already written

        log(rid, "info", "HEAD request satisfied without touching Telegram", {
            id,
            range: req.headers.range || "(full file)",
        });

        res.end();
    } catch (err) {
        log(rid, "error", "Unhandled error in HEAD stream route", {
            id,
            message: err.message,
            stack: err.stack,
        });
        if (!res.headersSent) {
            res.status(500).json({ error: "Failed to resolve video metadata" });
        } else if (!res.writableEnded) {
            res.end();
        }
    }
});

// ---------------------------------------------------------------------------
// GET /:id - actual byte streaming
// ---------------------------------------------------------------------------

router.get("/:id", async (req, res) => {
    const rid = reqId();
    const startedAt = Date.now();
    const { id } = req.params;

    let aborted = false;
    let abortReason = null;
    let downloadIterator = null;

    // If the client disconnects mid-stream (closes tab, seeks again, etc.)
    // stop pulling bytes from Telegram immediately.
    const onClose = () => {
        if (!aborted) {
            // res 'close' also fires after a normal, successful res.end(),
            // so only treat this as a genuine client-initiated abort if the
            // response hadn't actually finished yet.
            if (!res.writableEnded) {
                abortReason = "client_disconnected";
                log(rid, "warn", "Client connection closed before response finished", { id });
            }
        }
        aborted = true;
        if (downloadIterator && typeof downloadIterator.return === "function") {
            downloadIterator.return().catch(() => {});
        }
    };
    res.on("close", onClose);

    try {
        const target = await resolveStreamTarget(req, res, rid);
        if (!target) return; // error response already written

        const { client, video, media, totalSize, mimeType } = target;

        const parsed = applyRangeHeaders(req, res, rid, id, totalSize, mimeType);
        if (!parsed) return; // 416/400 already written

        const { start, end, contentLength, range } = parsed;

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
        // Stream from Telegram in chunks (no full-file buffering)
        // ---------------------------------------------------------------
        // MTProto file offsets/limits must be aligned to the chosen chunk
        // size (offset must be an exact multiple of the requestSize used),
        // so we download from the aligned start and trim the leading bytes
        // of the first chunk to land exactly on the requested `start`.
        const alignedStart = start - (start % CHUNK_SIZE);
        const skipBytes = start - alignedStart;
        const downloadWindowBytes = end - alignedStart + 1;

        // Explicit HTTP-Range -> Telegram-offset translation, logged so it
        // can be verified against the actual bytes that end up going out
        // (see "First bytes written to response" / SHA-256 sample below).
        // Invariant: alignedStart + skipBytes must equal `start` exactly.
        log(rid, "info", "Range-to-Telegram offset translation", {
            id,
            requestedStart: start,
            requestedEnd: end,
            alignedStart,
            skipBytes,
            downloadWindowBytes,
            chunkSize: CHUNK_SIZE,
        });

        // IMPORTANT: GramJS's iterDownload `limit` option is the NUMBER OF
        // CHUNK REQUESTS to issue (identical semantics to Telethon's
        // iter_download), NOT a byte count. Passing a raw byte count here
        // (as an even older version of this file did) is a silent
        // correctness bug: it happens to never *under*-provision chunks
        // mathematically, but it obscures the real chunk budget and made
        // it impossible to reason about / detect early termination. We
        // compute the real number of requests needed, plus a small safety
        // margin - the definitive safety net is the bytesSent ===
        // contentLength check after the loop, not this number.
        const chunkLimit = Math.ceil(downloadWindowBytes / CHUNK_SIZE) + 4;

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

        downloadIterator = client.iterDownload({
            file: fileLocation,
            offset: bigInt(alignedStart),
            limit: chunkLimit,
            requestSize: CHUNK_SIZE,
            dcId: media.dcId,
        });

        let bytesSent = 0;
        let rawBytesReceived = 0;
        // Cumulative raw bytes discarded so far in order to reach `start`.
        // Tracked incrementally across however many buffers iterDownload
        // actually yields (see the trimming comment below for why this
        // must NOT assume the first yielded buffer is one full
        // requestSize-aligned chunk).
        let skippedBytes = 0;
        let firstByteGlobalOffset = null;
        let lastActivity = Date.now();
        let chunkIndex = 0;

        // Bounded sample of the bytes actually written to the response,
        // starting at `start`, used to compute a SHA-256 for diagnostics
        // (see SHA256_SAMPLE_BYTES above).
        const hashSampleLimit = Math.min(SHA256_SAMPLE_BYTES, contentLength);
        const hashChunks = [];
        let hashBytesCollected = 0;

        const stallGuard = setInterval(() => {
            if (aborted) return;
            if (Date.now() - lastActivity > STALL_TIMEOUT_MS) {
                abortReason = "stall_timeout";
                log(rid, "error", "Stream stalled, aborting", {
                    id,
                    bytesSent,
                    stallTimeoutMs: STALL_TIMEOUT_MS,
                });
                aborted = true;
                if (downloadIterator && typeof downloadIterator.return === "function") {
                    downloadIterator.return().catch(() => {});
                }
            }
        }, 5000);

        try {
            for await (const chunk of downloadIterator) {
                if (aborted) break;

                chunkIndex += 1;
                rawBytesReceived += chunk.length;

                // A chunk shorter than requestSize before we've actually
                // reached the end of our requested window means Telegram
                // (or GramJS's own EOF heuristic) is cutting the download
                // short. GramJS's DownloadIter stops iterating entirely
                // the moment this happens, so this is the earliest point
                // we can flag the anomaly - the post-loop byte check below
                // is what actually prevents a corrupted response.
                if (chunk.length < CHUNK_SIZE && rawBytesReceived < downloadWindowBytes) {
                    log(rid, "warn", "Received a short chunk from Telegram before expected end of range", {
                        id,
                        chunkIndex,
                        chunkLength: chunk.length,
                        requestSize: CHUNK_SIZE,
                        rawBytesReceived,
                        downloadWindowBytes,
                    });
                }

                let piece = chunk;

                // ---------------------------------------------------------
                // Trim leading bytes down to the exact requested `start`.
                //
                // IMPORTANT: this must NOT assume the first buffer yielded
                // by iterDownload() is exactly one full requestSize-aligned
                // chunk. GramJS's DownloadIter is not contractually
                // guaranteed to yield buffers in lockstep with
                // `requestSize` - it can yield fewer/smaller buffers before
                // delivering a full chunk. The previous version trimmed
                // `skipBytes` off ONLY the very first yielded buffer via
                // `piece.subarray(skipBytes)`. If that first buffer was
                // ever shorter than `skipBytes`, `subarray()` silently
                // clamps to an empty buffer instead of throwing - so that
                // trim step would drop to nothing, and every buffer AFTER
                // it would then be written completely untrimmed, shifting
                // every subsequent byte in the response by however many
                // bytes were still owed. That produces exactly this
                // symptom: headers/duration parse fine (they don't depend
                // on this loop), but the actual media bytes are offset from
                // where the browser thinks they are, so decoding fails
                // partway in and the connection gets dropped.
                //
                // Fix: track cumulative raw bytes discarded so far
                // (`skippedBytes`) and only trim as much of THIS buffer as
                // is still owed, no matter how many buffers that spans.
                if (skippedBytes < skipBytes) {
                    const stillToSkip = skipBytes - skippedBytes;
                    if (chunk.length <= stillToSkip) {
                        // This whole buffer is still before `start` - drop
                        // it entirely and keep waiting.
                        skippedBytes += chunk.length;
                        piece = piece.subarray(0, 0);
                    } else {
                        piece = piece.subarray(stillToSkip);
                        skippedBytes += stillToSkip;
                    }
                }

                const remaining = contentLength - bytesSent;
                if (piece.length > remaining) {
                    piece = piece.subarray(0, remaining);
                }

                if (piece.length > 0) {
                    // Diagnostic: verify + log the global file offset of
                    // the very first byte we actually write, and its
                    // leading bytes. By construction this must equal
                    // `start` exactly (alignedStart + skippedBytes); if it
                    // doesn't, log it loudly rather than silently shipping
                    // misaligned bytes to the browser. For a start=0
                    // request the bytes should begin with a valid ISO-BMFF
                    // box (bytes 4-7 spelling "ftyp" for MP4).
                    if (firstByteGlobalOffset === null) {
                        firstByteGlobalOffset = alignedStart + skippedBytes;

                        if (firstByteGlobalOffset !== start) {
                            log(rid, "error", "ALIGNMENT MISMATCH: first written byte does not match requested Range start", {
                                id,
                                requestedStart: start,
                                firstByteGlobalOffset,
                                alignedStart,
                                skipBytes,
                                skippedBytes,
                            });
                        }

                        const preview = piece.subarray(0, Math.min(FIRST_BYTES_LOG_LENGTH, piece.length));
                        log(rid, "info", "First bytes written to response", {
                            id,
                            requestedStart: start,
                            firstByteGlobalOffset,
                            hex: preview.toString("hex"),
                            ascii: preview.toString("latin1").replace(/[^\x20-\x7e]/g, "."),
                        });
                    }

                    // Collect a bounded sample of the bytes actually
                    // written (starting at `start`) to hash for
                    // diagnostics - see SHA256_SAMPLE_BYTES.
                    if (hashBytesCollected < hashSampleLimit) {
                        const take = Math.min(piece.length, hashSampleLimit - hashBytesCollected);
                        hashChunks.push(Buffer.from(piece.subarray(0, take)));
                        hashBytesCollected += take;
                    }

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

        log(rid, "info", "Bytes written", { id, bytesSent, contentLength });

        if (hashChunks.length > 0) {
            const sampleBuffer = Buffer.concat(hashChunks, hashBytesCollected);
            const sha256 = crypto.createHash("sha256").update(sampleBuffer).digest("hex");
            log(rid, "info", "SHA-256 of streamed bytes (sample)", {
                id,
                sampleByteRange: `${start}-${start + hashBytesCollected - 1}`,
                sampleBytes: hashBytesCollected,
                sha256,
                howToVerify:
                    "Compare against sha256(dd if=<full downloaded copy of this video> " +
                    `bs=1 skip=${start} count=${hashBytesCollected} status=none) to confirm the ` +
                    "streamed bytes are byte-identical to the source file at this offset.",
            });
        }

        if (aborted) {
            log(rid, "warn", "Stream aborted before completion", {
                id,
                reason: abortReason || "unknown",
                bytesSent,
                contentLength,
                durationMs: Date.now() - startedAt,
            });
            if (!res.writableEnded) res.end();
            return;
        }

        // ---------------------------------------------------------------
        // Integrity check: never let a short/corrupted body look like
        // a successful response. If we didn't write exactly the number of
        // bytes we promised in Content-Length, destroy the connection
        // instead of calling res.end() normally.
        // ---------------------------------------------------------------
        if (bytesSent !== contentLength) {
            log(rid, "error", "BYTES-WRITTEN MISMATCH: bytesWritten !== Content-Length - aborting connection instead of ending it", {
                id,
                requestedRange: req.headers.range || "(full file)",
                actualRangeServed: `${start}-${end}`,
                contentLength,
                bytesWritten: bytesSent,
                difference: contentLength - bytesSent,
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

        log(rid, "info", "Stream finished", {
            id,
            requestedRange: req.headers.range || "(full file)",
            actualRangeServed: `${start}-${end}`,
            contentLength,
            bytesWritten: bytesSent,
            telegramDocumentSize: totalSize,
            durationMs: Date.now() - startedAt,
        });
    } catch (err) {
        abortReason = abortReason || "unhandled_error";
        log(rid, "error", "Unhandled error in stream route", {
            id,
            reason: abortReason,
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
        res.off("close", onClose);
    }
});

module.exports = router;
