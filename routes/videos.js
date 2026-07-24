
const express = require("express");
const router = express.Router();

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia, resolveMimeType } = require("../utils/telegram");

router.get("/:id", async (req, res) => {
    try {

        const client = req.app.locals.telegramClient;
        if (!client) {
            return res.status(503).json({ error: "Streaming service unavailable" });
        }

        const doc = await db.collection("videos").doc(req.params.id).get();

        if (!doc.exists) {
            return res.status(404).json({
                error: "Video not found"
            });
        }

        const video = doc.data();

        if (!video?.channelId || !video?.messageId) {
            return res.status(404).json({
                error: "Incomplete video metadata"
            });
        }

        const message = await getMessage(
            client,
            video.channelId,
            video.messageId
        );

        // Covers both a truly missing message and one that's been
        // deleted since it was saved - getMessage() already returns null
        // for both (see utils/telegram.js#fetchMessage), so a saved
        // record pointing at an old/deleted Telegram message surfaces
        // here as a clean 404 instead of the app silently trying to
        // stream a message that no longer exists.
        if (!message) {
            return res.status(404).json({
                error: "Telegram message not found"
            });
        }

        const media = getVideoMedia(message);

        if (!media) {
            return res.status(404).json({
                error: "Video media not found"
            });
        }

        // FIX: previously returned `media.mimeType` (whatever Telegram's
        // uploading client happened to declare - often generic/wrong,
        // e.g. "application/octet-stream" for a document-style upload)
        // instead of the same resolved MIME type /api/stream/:id
        // actually sends as the Content-Type header. If the app used
        // this field to decide how to play the video (e.g. setting a
        // <video type="..."> or a MIME-sniffing player), a mismatch here
        // - correct bytes served with a Content-Type the app thought was
        // different - is exactly what produces a "corrupted/broken
        // video" state in-app while the same file plays fine directly in
        // Telegram. Now uses the identical resolution logic as the
        // streaming route so both endpoints always agree.
        const { mimeType, fileName } = resolveMimeType(media);

        const attributes = media.attributes || [];
        const videoAttr = attributes.find((a) => a.className === "DocumentAttributeVideo");
        const hasPlayableVideoAttribute = !!videoAttr;

        res.json({
            success: true,
            title: video.title,
            category: video.category,
            season: video.season,
            episode: video.episode,
            // Full identity of the underlying Telegram file, so a saved
            // Firestore record can be cross-checked against what
            // Telegram is actually serving right now.
            telegramChannelId: video.channelId,
            telegramMessageId: video.messageId,
            telegramDocumentId: media.id.toString(),
            telegramAccessHash: media.accessHash ? media.accessHash.toString() : null,
            telegramDcId: media.dcId !== undefined ? media.dcId : null,
            hasFileReference: !!(media.fileReference && media.fileReference.length),
            fileName: fileName || null,
            mimeType,
            size: media.size ? Number(media.size.toString ? media.size.toString() : media.size) : null,
            duration: videoAttr ? videoAttr.duration : null,
            width: videoAttr ? videoAttr.w : null,
            height: videoAttr ? videoAttr.h : null,
            supportsStreaming: videoAttr ? !!videoAttr.supportsStreaming : null,
            // True only when Telegram's own DocumentAttributeVideo is
            // present on the stored document - the same signal the
            // upload pipeline checks to confirm Telegram accepted this
            // as playable video rather than a generic document. If this
            // is false, the file will not play inline anywhere,
            // including in this app, regardless of what /api/stream/:id
            // returns.
            hasPlayableVideoAttribute,
            // Canonical, always-correct playback URL for this video -
            // removes any need (and any chance of drift/mistakes) for
            // client code to hand-build this path itself.
            streamUrl: `/api/stream/${req.params.id}`
        });

    } catch (err) {

        console.error(err);

        res.status(500).json({
            error: err.message
        });

    }
});

module.exports = router;
