
const express = require("express");
const router = express.Router();

const { db } = require("../services/firebase");
const { getMessage, getVideoMedia } = require("../utils/telegram");

router.get("/:id", async (req, res) => {
    try {

        const client = req.app.locals.telegramClient;

        const doc = await db.collection("videos").doc(req.params.id).get();

        if (!doc.exists) {
            return res.status(404).json({
                error: "Video not found"
            });
        }

        const video = doc.data();

        const message = await getMessage(
            client,
            video.channelId,
            video.messageId
        );

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

        res.json({
            success: true,
            title: video.title,
            category: video.category,
            season: video.season,
            episode: video.episode,
            telegramDocumentId: media.id.toString(),
            mimeType: media.mimeType,
            size: media.size
        });

    } catch (err) {

        console.error(err);

        res.status(500).json({
            error: err.message
        });

    }
});

module.exports = router;
