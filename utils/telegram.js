const { Api } = require("telegram");

const MAX_MESSAGE_ATTEMPTS = 4;
const BASE_RETRY_DELAY_MS = 800;
const DIALOG_SYNC_COOLDOWN_MS = 30 * 1000;
const DIALOG_SYNC_LIMIT = 300;

const peerCache = new Map();

let lastDialogSyncAt = 0;
let dialogSyncInFlight = null;

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

function describeError(err) {
    if (!err) return "Unknown error";
    if (err.errorMessage) return err.errorMessage;
    if (err.code && err.message) return `${err.code}: ${err.message}`;
    return err.message || String(err);
}

function normalizeChannelId(channelId) {
    if (typeof channelId === "string" && channelId.trim().startsWith("@")) {
        return channelId.trim();
    }

    const idStr = String(channelId).trim();

    if (idStr.startsWith("-100")) {
        return idStr;
    }

    if (idStr.startsWith("-")) {
        return idStr;
    }

    return `-100${idStr}`;
}

async function syncDialogs(client, { force = false } = {}) {
    const now = Date.now();

    if (!force && now - lastDialogSyncAt < DIALOG_SYNC_COOLDOWN_MS) {
        return;
    }

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

async function resolveChannel(client, channelId) {
    const key = normalizeChannelId(channelId);

    const cached = peerCache.get(key);
    if (cached) {
        return cached;
    }

    console.log(`[telegram] Resolving channel... (raw=${channelId}, normalized=${key})`);

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

async function fetchMessage(client, channelId, peer, messageId) {
    let lastErr = null;

    for (let attempt = 1; attempt <= MAX_MESSAGE_ATTEMPTS; attempt++) {
        try {
            console.log(
                `[telegram] Loading Telegram message... (channel=${channelId}, messageId=${messageId}, attempt=${attempt}/${MAX_MESSAGE_ATTEMPTS})`
            );

            const messages = await client.getMessages(peer, { ids: [messageId] });
            const message = messages?.[0];

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

            const finalErr = new Error(`TELEGRAM_RPC_ERROR: ${detail}`);
            finalErr.telegramCode = detail;
            throw finalErr;
        }
    }

    const err = new Error(`TELEGRAM_RPC_ERROR: ${describeError(lastErr)}`);
    err.telegramCode = describeError(lastErr);
    throw err;
}

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

    const peer = await resolveChannel(client, channelId);

    return fetchMessage(client, channelId, peer, numericMessageId);
}

function sizeToNumber(value) {
    if (value === null || value === undefined) return NaN;
    if (typeof value === "number") return value;
    if (typeof value === "bigint") return Number(value);
    if (typeof value.toJSNumber === "function") return value.toJSNumber();
    return Number(value.toString());
}

function getFileLocation(doc) {
    if (!doc || doc.className !== "Document") {
        throw new Error("getFileLocation requires a real Api.Document");
    }

    return new Api.InputDocumentFileLocation({
        id: doc.id,
        accessHash: doc.accessHash,
        fileReference: doc.fileReference,
        thumbSize: "", 
    });
}

function getVideoMedia(message) {
    if (!message || !message.media) {
        return null;
    }

    const media = message.media;

    if (media.className !== "MessageMediaDocument" || !media.document) {
        return null;
    }

    const doc = media.document;

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

function extractFileName(doc) {
    const attr = (doc?.attributes || []).find((a) => a.className === "DocumentAttributeFilename");
    return attr?.fileName || null;
}

/**
 * Telegram echoes back whatever mime type the UPLOADING client declared
 * at send time, which is frequently wrong or generic
 * ("application/octet-stream" is common for files sent as a plain
 * Document rather than through the video picker). The previous version
 * of this logic trusted that label outright whenever it happened to
 * start with "video/", and forced everything else to "video/mp4" -
 * which is exactly what turns a real MKV/WebM file into a broken/
 * corrupted icon in Chrome: the browser is told "this is an MP4" and
 * tries to parse an MP4 box structure against actual Matroska/WebM EBML
 * data (or the reverse), and the demuxer fails outright.
 *
 * The filename extension is the most reliable signal available - it's
 * literally the name the file was uploaded with - so it's checked FIRST
 * and wins on conflict with the declared mime. The original container is
 * always preserved; "video/mp4" is only ever used as an absolute last
 * resort when neither the filename nor the mime gives a real answer.
 */
function resolveMimeType(doc) {
    const fileName = extractFileName(doc);
    const name = (fileName || "").toLowerCase();
    const rawMime = (doc && doc.mimeType) || "";
    const mime = rawMime.toLowerCase();

    if (/\.mkv$/.test(name)) return { mimeType: "video/x-matroska", fileName };
    if (/\.webm$/.test(name)) return { mimeType: "video/webm", fileName };
    if (/\.mov$/.test(name)) return { mimeType: "video/quicktime", fileName };
    if (/\.avi$/.test(name)) return { mimeType: "video/x-msvideo", fileName };
    if (/\.(m4v|mp4)$/.test(name)) return { mimeType: "video/mp4", fileName };
    if (/\.ts$/.test(name) || /\.m2ts$/.test(name)) return { mimeType: "video/mp2t", fileName };
    if (/\.flv$/.test(name)) return { mimeType: "video/x-flv", fileName };

    if (/matroska/.test(mime)) return { mimeType: "video/x-matroska", fileName };
    if (/webm/.test(mime)) return { mimeType: "video/webm", fileName };
    if (/quicktime/.test(mime)) return { mimeType: "video/quicktime", fileName };
    if (/x-msvideo/.test(mime)) return { mimeType: "video/x-msvideo", fileName };
    if (mime.startsWith("video/") && mime !== "video/octet-stream") return { mimeType: rawMime, fileName };

    return { mimeType: "video/mp4", fileName }; // no reliable signal at all
}

module.exports = {
    getMessage,
    getVideoMedia,
    getFileLocation,
    resolveChannel,
    invalidateChannelCache,
    extractFileName,
    resolveMimeType,
};
