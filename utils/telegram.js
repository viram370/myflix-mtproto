/**
 * utils/telegram.js
 *
 * Robust Telegram (GramJS / MTProto) message + media resolution helpers.
 *
 * Root cause of the previous "Could not play video" / "Source message not
 * found" bug: getMessage() called client.getEntity(channelId) with no
 * entity/InputPeer caching and no fallback path. MTProto channels require an
 * access_hash to build a valid InputPeerChannel/InputChannel; GramJS only
 * knows that access_hash if the entity has already been seen in this
 * session (e.g. via a prior getDialogs() call). A "cold" session that has
 * never listed its dialogs will fail to resolve a channel purely from a
 * numeric ID, even though the channel and message both genuinely exist.
 * On top of that, the file had a duplicated/nested function body that put a
 * `module.exports = {...}` assignment *inside* getMessage's function body,
 * so the real exported getMessage was a broken, effectively-no-op shadow
 * function that always returned undefined.
 *
 * This file fixes both issues:
 *  1. Normalizes channelId (handles -100xxxxxxxxxx marked IDs, bare
 *     positive channel IDs, and @usernames).
 *  2. Resolves + caches InputPeer entities, and if the entity is not yet
 *     known to the session, forces a dialog sync (which populates GramJS's
 *     entity cache with access hashes) and retries automatically.
 *  3. Retries transient Telegram/RPC failures (including FLOOD_WAIT) with
 *     backoff, and only ever returns `null` when the message truly does
 *     not exist. Genuine connectivity / resolution failures are thrown so
 *     callers (routes/stream.js) can correctly report "unavailable" rather
 *     than incorrectly reporting "not found".
 *  4. Detects video media on both Api.Document (video attribute) and
 *     video/* mime-typed documents, including forwarded messages, and
 *     refuses to hand back a document with a missing/invalid size (which
 *     would otherwise silently break the byte-range math downstream in
 *     routes/stream.js and produce a corrupted, unplayable response).
 */

const { Api } = require("telegram");

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const MAX_MESSAGE_ATTEMPTS = 4;
const BASE_RETRY_DELAY_MS = 800;
const DIALOG_SYNC_COOLDOWN_MS = 30 * 1000; // don't hammer Telegram on repeated misses
const DIALOG_SYNC_LIMIT = 300;

// ---------------------------------------------------------------------------
// Entity / InputPeer cache
// ---------------------------------------------------------------------------

// normalizedChannelId -> resolved InputPeer
const peerCache = new Map();

// Timestamp of the last forced dialog sync, so concurrent/rapid misses don't
// each trigger their own full getDialogs() call.
let lastDialogSyncAt = 0;
let dialogSyncInFlight = null;

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Extract a stable, human-readable Telegram/RPC error code such as
 * CHANNEL_INVALID, CHAT_ID_INVALID, MESSAGE_ID_INVALID, PEER_ID_INVALID,
 * or FLOOD_WAIT_X. Falls back to the generic error message.
 */
function describeError(err) {
    if (!err) return "Unknown error";
    if (err.errorMessage) return err.errorMessage; // GramJS RPCError code, e.g. CHANNEL_INVALID
    if (err.code && err.message) return `${err.code}: ${err.message}`;
    return err.message || String(err);
}

/**
 * Normalize a Firestore-stored channelId into the form GramJS expects.
 * Accepts:
 *   -1001234567890   (Bot-API "marked" channel id, number or string)
 *   1234567890        (bare positive channel id)
 *   "@somechannel"     (public username)
 */
function normalizeChannelId(channelId) {
    if (typeof channelId === "string" && channelId.trim().startsWith("@")) {
        return channelId.trim();
    }

    const idStr = String(channelId).trim();

    if (idStr.startsWith("-100")) {
        // Already in marked form, GramJS understands this directly.
        return idStr;
    }

    if (idStr.startsWith("-")) {
        // Some other negative form (e.g. basic group) - use as-is.
        return idStr;
    }

    // Bare positive channel id -> convert to the standard marked form.
    return `-100${idStr}`;
}

/**
 * Force a dialog listing so GramJS populates its session entity cache
 * (id -> access_hash) for every chat/channel/user this account can see.
 * This is the standard fix for "Could not find the input entity" errors.
 */
async function syncDialogs(client, { force = false } = {}) {
    const now = Date.now();

    if (!force && now - lastDialogSyncAt < DIALOG_SYNC_COOLDOWN_MS) {
        return;
    }

    // Coalesce concurrent callers into a single in-flight sync.
    if (dialogSyncInFlight) {
        return dialogSyncInFlight;
    }

    lastDialogSyncAt = now;

    dialogSyncInFlight = (async () => {
        console.log("[telegram] Syncing dialogs to warm entity cache...");
        try {
            const dialogs = await client.getDialogs({ limit: DIALOG_SYNC_LIMIT });
            console.log(`[telegram] Dialog sync complete (${dialogs.length} dialogs cached).`);
        } catch (err) {
            console.error("[telegram] Dialog sync failed:", describeError(err));
        } finally {
            dialogSyncInFlight = null;
        }
    })();

    return dialogSyncInFlight;
}

/**
 * Resolve a channelId into a usable InputPeer, using the cache first,
 * then falling back to a forced dialog sync + retry. Throws (does not
 * return null) if the channel genuinely cannot be resolved, so callers can
 * distinguish "Telegram unavailable / not resolvable" from "message not
 * found".
 */
async function resolveChannel(client, channelId) {
    const key = normalizeChannelId(channelId);

    const cached = peerCache.get(key);
    if (cached) {
        return cached;
    }

    console.log(`[telegram] Resolving channel... (raw=${channelId}, normalized=${key})`);

    // Attempt 1: direct resolution (works if already in the session's
    // entity cache, or if key is a public @username).
    try {
        const inputPeer = await client.getInputEntity(key);
        peerCache.set(key, inputPeer);
        console.log(`[telegram] Resolved peer for ${key}.`);
        return inputPeer;
    } catch (firstErr) {
        console.warn(
            `[telegram] Direct entity resolution failed for ${key}: ${describeError(firstErr)}. ` +
                "Retrying after dialog sync..."
        );
    }

    // Attempt 2: force a dialog sync (loads access hashes) and retry.
    await syncDialogs(client, { force: true });

    try {
        const inputPeer = await client.getInputEntity(key);
        peerCache.set(key, inputPeer);
        console.log(`[telegram] Resolved peer for ${key} after dialog sync.`);
        return inputPeer;
    } catch (secondErr) {
        const detail = describeError(secondErr);
        console.error(`[telegram] Channel resolution failed for ${key}: ${detail}`);

        const err = new Error(
            `CHANNEL_UNRESOLVED: could not resolve channel ${channelId} (${detail}). ` +
                "The Telegram account behind this session may not be a member of this channel, " +
                "or its entity cache is stale."
        );
        err.telegramCode = detail;
        err.cause = secondErr;
        throw err;
    }
}

/**
 * Drop a cached peer, e.g. after a CHANNEL_INVALID / PEER_ID_INVALID
 * response, in case the access hash changed or the cache went stale.
 */
function invalidateChannelCache(channelId) {
    const key = normalizeChannelId(channelId);
    peerCache.delete(key);
}

function isFloodWaitError(err) {
    return err && (err.className === "FloodWaitError" || typeof err.seconds === "number");
}

function isRetryableCode(detail) {
    if (!detail) return false;
    return /TIMEOUT|CONNECTION|ECONNRESET|ETIMEDOUT|EAI_AGAIN|-500|-503|INTERNAL/i.test(detail);
}

function isStaleEntityCode(detail) {
    if (!detail) return false;
    return /CHANNEL_INVALID|PEER_ID_INVALID|CHAT_ID_INVALID/i.test(detail);
}

/**
 * Fetch a single message by id from an already-resolved peer, with retry +
 * backoff for transient failures (including FLOOD_WAIT).
 */
async function fetchMessage(client, channelId, peer, messageId) {
    let lastErr = null;

    for (let attempt = 1; attempt <= MAX_MESSAGE_ATTEMPTS; attempt++) {
        try {
            console.log(
                `[telegram] Loading Telegram message... (channel=${channelId}, messageId=${messageId}, attempt=${attempt}/${MAX_MESSAGE_ATTEMPTS})`
            );

            const messages = await client.getMessages(peer, { ids: [messageId] });
            const message = messages?.[0];

            // GramJS returns an Api.MessageEmpty (or nothing) when the id is
            // valid-shaped but the message truly doesn't exist / was deleted.
            if (!message || message.className === "MessageEmpty") {
                console.warn(
                    `[telegram] Message ${messageId} does not exist in channel ${channelId} (deleted or never existed).`
                );
                return null;
            }

            console.log(`[telegram] Message found (id=${message.id}).`);
            return message;
        } catch (err) {
            lastErr = err;
            const detail = describeError(err);
            console.error(
                `[telegram] getMessages failed (channel=${channelId}, messageId=${messageId}, attempt=${attempt}): ${detail}`
            );

            if (isStaleEntityCode(detail)) {
                // The cached peer is no longer valid - purge it so the next
                // top-level getMessage() call re-resolves from scratch.
                invalidateChannelCache(channelId);
                const reErr = new Error(`TELEGRAM_RPC_ERROR: ${detail}`);
                reErr.telegramCode = detail;
                throw reErr;
            }

            if (isFloodWaitError(err)) {
                const waitMs = (err.seconds || 5) * 1000;
                console.warn(`[telegram] FLOOD_WAIT hit, waiting ${waitMs}ms before retry...`);
                await sleep(waitMs);
                continue;
            }

            if (attempt < MAX_MESSAGE_ATTEMPTS && isRetryableCode(detail)) {
                const delay = BASE_RETRY_DELAY_MS * attempt;
                console.warn(`[telegram] Transient error, retrying in ${delay}ms...`);
                await sleep(delay);
                continue;
            }

            // Non-retryable RPC error (MESSAGE_ID_INVALID, etc.) - surface it.
            const finalErr = new Error(`TELEGRAM_RPC_ERROR: ${detail}`);
            finalErr.telegramCode = detail;
            throw finalErr;
        }
    }

    const err = new Error(`TELEGRAM_RPC_ERROR: ${describeError(lastErr)}`);
    err.telegramCode = describeError(lastErr);
    throw err;
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/**
 * Get a message from a Telegram channel.
 *
 * Returns the Api.Message on success.
 * Returns null ONLY when the message genuinely does not exist (bad id,
 * deleted message, or MessageEmpty).
 * Throws when the channel/entity cannot be resolved or Telegram is
 * unavailable, so callers can distinguish "unavailable" from "not found".
 */
async function getMessage(client, channelId, messageId) {
    if (!client) {
        throw new Error("TELEGRAM_CLIENT_MISSING: no Telegram client provided");
    }

    if (!channelId) {
        console.error("[telegram] getMessage called without a channelId");
        return null;
    }

    const numericMessageId = Number(messageId);
    if (!Number.isFinite(numericMessageId) || numericMessageId <= 0) {
        console.error(`[telegram] Invalid messageId provided: ${messageId}`);
        return null;
    }

    // Resolve the peer; this throws (not returns null) on real failures.
    const peer = await resolveChannel(client, channelId);

    // Fetch the message; this also throws on real failures, returns null
    // only when the message truly does not exist.
    return fetchMessage(client, channelId, peer, numericMessageId);
}

/**
 * Best-effort conversion of a Document's `size` field (Number / BigInt /
 * big-integer instance) to a plain JS Number, purely for validation/logging
 * here. routes/stream.js does its own equivalent conversion for the actual
 * byte-range math.
 */
function sizeToNumber(value) {
    if (value === null || value === undefined) return NaN;
    if (typeof value === "number") return value;
    if (typeof value === "bigint") return Number(value);
    if (typeof value.toJSNumber === "function") return value.toJSNumber();
    return Number(value.toString());
}

/**
 * Check if a message contains playable video and return the underlying
 * Api.Document. Handles:
 *   - Native videos (DocumentAttributeVideo)
 *   - Documents uploaded as video/* mime type (no video attribute)
 *   - Forwarded messages (media shape is identical to originals in MTProto)
 *
 * Only ever returns a Document that is genuinely playable: it must be a
 * real Api.Document (not DocumentEmpty) with a valid, positive size. A
 * malformed/sizeless document would otherwise pass silently through here
 * and only surface as a confusing failure deep in the byte-range math in
 * routes/stream.js - better to catch and log it at the source.
 */
function getVideoMedia(message) {
    if (!message || !message.media) {
        return null;
    }

    const media = message.media;

    if (media.className !== "MessageMediaDocument" || !media.document) {
        return null;
    }

    const doc = media.document;

    // DocumentEmpty has no attributes/mimeType/size - not playable.
    if (doc.className !== "Document") {
        console.warn(`[telegram] Message media document is not a real Document (className=${doc.className}).`);
        return null;
    }

    const attributes = doc.attributes || [];
    const hasVideoAttribute = attributes.some(
        (attr) => attr.className === "DocumentAttributeVideo"
    );
    const isVideoMime = typeof doc.mimeType === "string" && doc.mimeType.startsWith("video/");
    const isAnimatedOnly = attributes.some((attr) => attr.className === "DocumentAttributeAnimated");

    if (!hasVideoAttribute && !isVideoMime) {
        return null;
    }

    const size = sizeToNumber(doc.size);
    if (!Number.isFinite(size) || size <= 0) {
        console.error(
            `[telegram] Video document found but has no valid size (id=${doc.id}, rawSize=${doc.size}). Refusing to hand it off for streaming.`
        );
        return null;
    }

    console.log(
        `[telegram] Video media found (id=${doc.id}, mime=${doc.mimeType}, size=${size}, animatedOnly=${isAnimatedOnly}).`
    );
    return doc;
}

module.exports = {
    getMessage,
    getVideoMedia,
    // Exposed for diagnostics / potential reuse elsewhere.
    resolveChannel,
    invalidateChannelCache,
};
