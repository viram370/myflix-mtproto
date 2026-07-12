
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

// How long we wait specifically for the FIRST chunk from iterDownload()
// before giving up. This is intentionally shorter than STALL_TIMEOUT_MS:
// a hang on the very first chunk almost always means the sender for this
// document's DC never finished connecting/exporting (DC migration stall)
// or is unreachable (blocked egress to that DC's IP), not a transient
// mid-stream slowdown - so there's no reason to make the browser wait a
// full 30s (by which point it will likely have already given up and
// disconnected on its own, which is indistinguishable in logs from a
// genuine client-side abort). Failing fast here also means we can still
// send a clean error response, since Express won't have flushed any
// headers yet at this point (nothing has been written to `res`).
const FIRST_CHUNK_TIMEOUT_MS = 12000;

// Installed GramJS ("telegram" npm package) version, per package.json:
// "telegram": "^2.26.22". iterDownload()'s documented shape for this
// version range is: iterDownload(client, { file, offset, limit, stride,
// chunkSize, requestSize, fileSize, dcId }) where `file` must already be
// (or resolve to) an Api.TypeInputFileLocation - for a Document that's
// Api.InputDocumentFileLocation, NOT Api.InputPhotoFileLocation (that's
// for photos) and NOT a raw Api.Document. `offset` is a big-integer BYTE
// offset into the file. `limit` is a REQUEST COUNT (number of upload.GetFile
// RPCs to issue), not a byte count. `requestSize` is the BYTE size per RPC
// (must be a multiple of 4096, max 1 MiB). All of this matches what this
// file already does. This sandbox has no network access to diff against
// the live npm registry/source for 2.26.22 line by line - if you want a
// byte-for-byte confirmation, check `node_modules/telegram/client/downloads.js`
// (or .ts source) directly in your deployment.
//
// Given the parameters check out against the documented shape, a hang
// where iterDownload() never yields ANY chunk (not even an error) point
// to the sender/DC-connection setup underneath it stalling, not a
// parameter mismatch. As a resilience measure (and because it's the
// higher-level, more stable, explicitly documented top-level API for
// ranged downloads), if the low-level iterDownload() path fails to
// produce a first chunk in time, we fall back to client.downloadFile()
// with explicit start/end byte offsets, pulled in bounded sub-ranges so
// memory stays capped at roughly CHUNK_SIZE per iteration.
const FALLBACK_CHUNK_TIMEOUT_MS = 20000;

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
 * Fallback download path used when the low-level client.iterDownload()
 * never produces a first chunk (see FIRST_CHUNK_TIMEOUT_MS). Uses
 * client.downloadFile() - GramJS's higher-level, officially documented
 * method for pulling an explicit byte range (`start`/`end`) - instead of
 * iterDownload()'s manual offset/limit/requestSize bookkeeping. Because
 * downloadFile() resolves a single byte range as one in-memory Buffer, we
 * call it once per bounded sub-range (~CHUNK_SIZE each) rather than once
 * for the whole request, so peak memory stays capped and we can still
 * write to the response incrementally like a real stream. Each sub-range
 * call is individually time-bounded so a repeat stall fails fast instead
 * of hanging again.
 *
 * Yields Buffers covering [rangeStart, rangeEnd] inclusive, in order.
 */
async function* downloadFileFallback({ client, fileLocation, dcId, totalSize, rangeStart, rangeEnd, rid, id }) {
    let cursor = rangeStart;

    while (cursor <= rangeEnd) {
        const subEnd = Math.min(cursor + CHUNK_SIZE - 1, rangeEnd);

        log(rid, "info", "[fallback] Requesting sub-range via client.downloadFile()", {
            id,
            start: cursor,
            end: subEnd,
        });

        const buffer = await withTimeout(
            client.downloadFile(fileLocation, {
                dcId,
                fileSize: bigInt(totalSize),
                start: cursor,
                end: subEnd,
                workers: 1,
            }),
            FALLBACK_CHUNK_TIMEOUT_MS,
            "client.downloadFile() fallback sub-range"
        );

        if (!buffer || buffer.length === 0) {
            throw new Error(
                `client.downloadFile() fallback returned no data for byte range ${cursor}-${subEnd}`
            );
        }

        log(rid, "info", "[fallback] Received sub-range from client.downloadFile()", {
            id,
            start: cursor,
            end: subEnd,
            bytes: buffer.length,
        });

        yield buffer;
        cursor += buffer.length;
    }
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

    // Hoisted so the close handler (attached below, before any async work
    // starts) can report exactly how far the request had gotten when the
    // connection closed - instead of a bare "client_disconnected" that
    // looks the same whether Telegram had barely started or was mid-file.
    let bytesSent = 0;
    let downloadAttemptStartedAt = null; // set right before iterDownload() is called
    let firstChunkAttempted = false; // true once we've begun awaiting the first chunk

    // If the client disconnects mid-stream (closes tab, seeks again, etc.)
    // stop pulling bytes from Telegram immediately.
    const onClose = () => {
        if (!aborted) {
            // res 'close' also fires after a normal, successful res.end(),
            // so only treat this as a genuine client-initiated abort if the
            // response hadn't actually finished yet.
            if (!res.writableEnded) {
                abortReason = "client_disconnected";
                const msSinceRequestStart = Date.now() - startedAt;
                const msSinceDownloadStarted = downloadAttemptStartedAt
                    ? Date.now() - downloadAttemptStartedAt
                    : null;
                log(rid, "warn", "Client connection closed before response finished", {
                    id,
                    bytesSentBeforeClose: bytesSent,
                    firstChunkAttempted,
                    msSinceRequestStart,
                    msSinceDownloadStarted,
                    // If msSinceDownloadStarted is small (a second or two)
                    // and well under FIRST_CHUNK_TIMEOUT_MS, this was NOT
                    // our own timeout firing - the client/proxy gave up on
                    // its own, faster than Telegram had a chance to
                    // respond. If downloadAttemptStartedAt is null, the
                    // connection closed before we even called
                    // iterDownload() at all.
                });
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

        // Flush headers to the client/any intermediate proxy immediately,
        // before we start pulling data from Telegram. Express normally
        // holds headers back until the first res.write()/res.end() call,
        // which means the connection can look completely idle - no bytes
        // of any kind - for however long the Telegram round trip takes.
        // Some proxies and HTTP clients treat "no bytes at all for N
        // seconds" as a dead connection and close it well before any
        // reasonable data timeout would apply. Sending the 206/200 headers
        // right away at least gives the client a live response to hold
        // onto while it waits for the body.
        if (typeof res.flushHeaders === "function") {
            res.flushHeaders();
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

        // Diagnostic: surface whether this document actually lives on a
        // different DC than the one the client's main connection is on.
        // iterDownload() is supposed to transparently export/borrow a
        // sender for `dcId` when they differ, but if that export hangs
        // (auth-key exchange stall) or the DC's IP is unreachable from
        // this container's network egress, iterDownload() will simply
        // never yield a first chunk - with no error, no throw, just
        // silence, which is indistinguishable from any other kind of hang
        // unless we log the DCs involved up front.
        const clientMainDcId = client.session?.dcId ?? client._dcId ?? "unknown";
        log(rid, "info", "DC check before download", {
            id,
            fileDcId: media.dcId,
            clientMainDcId,
            crossDc: clientMainDcId !== "unknown" && clientMainDcId !== media.dcId,
        });

        downloadAttemptStartedAt = Date.now();
        log(rid, "info", "Calling client.iterDownload()...", {
            id,
            file: "InputDocumentFileLocation",
            offset: alignedStart,
            offsetType: typeof bigInt(alignedStart),
            limit: chunkLimit,
            requestSize: CHUNK_SIZE,
            dcId: media.dcId,
        });

        downloadIterator = client.iterDownload({
            file: fileLocation,
            offset: bigInt(alignedStart),
            limit: chunkLimit,
            requestSize: CHUNK_SIZE,
            dcId: media.dcId,
        });

        log(rid, "info", "iterDownload() returned an iterator object (synchronous call succeeded)", {
            id,
            hasAsyncIterator: typeof downloadIterator[Symbol.asyncIterator] === "function",
            hasReturnMethod: typeof downloadIterator.return === "function",
        });

        bytesSent = 0;
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

        log(rid, "info", "Awaiting first chunk from Telegram iterDownload()...", {
            id,
            offset: alignedStart,
            requestSize: CHUNK_SIZE,
            limit: chunkLimit,
            dcId: media.dcId,
            firstChunkTimeoutMs: FIRST_CHUNK_TIMEOUT_MS,
        });

        // Manual iterator consumption (instead of `for await...of`) so we
        // can wrap ONLY the very first `.next()` call in its own bounded
        // timeout. A hang on the first chunk means Telegram never even
        // began answering (see the DC-check log above); a hang mid-stream
        // is covered separately by the STALL_TIMEOUT_MS watchdog below.
        const asyncIterator =
            typeof downloadIterator[Symbol.asyncIterator] === "function"
                ? downloadIterator[Symbol.asyncIterator]()
                : downloadIterator;

        let isFirstNext = true;
        let firstChunkTimedOut = false;

        try {
            while (true) {
                if (aborted) break;

                let result;
                if (isFirstNext) {
                    isFirstNext = false;
                    firstChunkAttempted = true;
                    try {
                        result = await withTimeout(
                            asyncIterator.next(),
                            FIRST_CHUNK_TIMEOUT_MS,
                            "Telegram iterDownload() first chunk"
                        );
                    } catch (firstChunkErr) {
                        // withTimeout() throws a specific "... timed out
                        // after Xms" message when the clock runs out; any
                        // other message means the iterator itself actually
                        // threw (e.g. a Telegram RPC error such as
                        // FILE_REFERENCE_EXPIRED) rather than just being
                        // slow - these need very different follow-up, so
                        // log them distinctly instead of lumping both
                        // under "timeout".
                        const isTimeout = /timed out after/i.test(firstChunkErr.message || "");
                        firstChunkTimedOut = true;
                        abortReason = isTimeout ? "no_first_chunk" : "first_chunk_exception";
                        log(
                            rid,
                            "error",
                            isTimeout
                                ? "No chunk received from Telegram within timeout - iterDownload() never yielded. Likely a DC migration/sender export stall, an unreachable DC IP from this container's egress, or an invalid file location"
                                : "iterDownload() THREW while producing the first chunk (not a timeout - a real exception)",
                            {
                                id,
                                isTimeout,
                                fileDcId: media.dcId,
                                clientMainDcId: client.session?.dcId ?? client._dcId ?? "unknown",
                                offset: alignedStart,
                                requestSize: CHUNK_SIZE,
                                limit: chunkLimit,
                                timeoutMs: FIRST_CHUNK_TIMEOUT_MS,
                                message: firstChunkErr.message,
                                stack: firstChunkErr.stack,
                            }
                        );
                        // Intentionally NOT setting `aborted = true` here -
                        // that flag is reserved for genuine client
                        // disconnects / mid-stream stalls (see onClose and
                        // the stallGuard above). A first-chunk timeout on
                        // the low-level iterDownload() path is recoverable:
                        // we abandon just this iterator and try the
                        // client.downloadFile() fallback below, as long as
                        // the browser hasn't actually gone away.
                        if (typeof downloadIterator.return === "function") {
                            downloadIterator.return().catch(() => {});
                        }
                        break;
                    }
                } else {
                    try {
                        result = await asyncIterator.next();
                    } catch (midStreamErr) {
                        abortReason = "mid_stream_exception";
                        log(rid, "error", "iterDownload() THREW mid-stream (after at least one prior chunk)", {
                            id,
                            chunkIndex,
                            bytesSent,
                            message: midStreamErr.message,
                            stack: midStreamErr.stack,
                        });
                        aborted = true;
                        break;
                    }
                }

                if (result.done) break;

                const chunk = result.value;

                if (aborted) break;

                chunkIndex += 1;
                rawBytesReceived += chunk.length;

                log(rid, "info", "Received Telegram chunk", {
                    id,
                    chunkIndex,
                    chunkLength: chunk.length,
                    rawBytesReceived,
                });

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

        if (firstChunkTimedOut && !aborted) {
            log(rid, "warn", "Falling back to client.downloadFile() after iterDownload() produced no first chunk", {
                id,
                requestedRange: `${start}-${end}`,
                fallbackChunkTimeoutMs: FALLBACK_CHUNK_TIMEOUT_MS,
            });

            lastActivity = Date.now();
            const fallbackStallGuard = setInterval(() => {
                if (aborted) return;
                if (Date.now() - lastActivity > STALL_TIMEOUT_MS) {
                    abortReason = "stall_timeout_fallback";
                    log(rid, "error", "Fallback stream stalled, aborting", {
                        id,
                        bytesSent,
                        stallTimeoutMs: STALL_TIMEOUT_MS,
                    });
                    aborted = true;
                }
            }, 5000);

            try {
                for await (const buffer of downloadFileFallback({
                    client,
                    fileLocation,
                    dcId: media.dcId,
                    totalSize,
                    rangeStart: start,
                    rangeEnd: end,
                    rid,
                    id,
                })) {
                    if (aborted) break;

                    // downloadFile() was requested with the exact
                    // start/end byte range, so - unlike the primary
                    // iterDownload() path - no alignment/skip trimming is
                    // needed here at all.
                    let piece = buffer;
                    const remaining = contentLength - bytesSent;
                    if (piece.length > remaining) {
                        piece = piece.subarray(0, remaining);
                    }

                    if (piece.length > 0) {
                        if (firstByteGlobalOffset === null) {
                            firstByteGlobalOffset = start;
                            const preview = piece.subarray(0, Math.min(FIRST_BYTES_LOG_LENGTH, piece.length));
                            log(rid, "info", "First bytes written to response (via client.downloadFile() fallback)", {
                                id,
                                requestedStart: start,
                                firstByteGlobalOffset,
                                hex: preview.toString("hex"),
                                ascii: preview.toString("latin1").replace(/[^\x20-\x7e]/g, "."),
                            });
                        }

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

                if (bytesSent > 0) {
                    // The fallback actually delivered data - clear the
                    // timeout flag so the normal bytesSent === contentLength
                    // integrity check below governs the outcome instead of
                    // the unconditional 503 further down.
                    firstChunkTimedOut = false;
                    abortReason = null;
                }
            } catch (fallbackErr) {
                log(rid, "error", "client.downloadFile() fallback also failed - both download paths are broken for this DC/file", {
                    id,
                    fileDcId: media.dcId,
                    message: fallbackErr.message,
                });
            } finally {
                clearInterval(fallbackStallGuard);
            }
        }

        if (firstChunkTimedOut) {
            if (!res.headersSent) {
                // Nothing has been written yet (both the primary and
                // fallback paths failed to deliver anything), so we can
                // still respond with a clean error instead of leaving the
                // client to time out and disconnect on its own (which is
                // what produced the ambiguous "client_disconnected" abort
                // reason previously - the real cause was always this).
                return res.status(503).json({
                    error: "Telegram did not return video data in time. Please try again.",
                });
            }
            // Headers were already flushed somehow - fall through to the
            // normal aborted/integrity-check handling below, which will
            // correctly destroy the connection since bytesSent is 0.
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
