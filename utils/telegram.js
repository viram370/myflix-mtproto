
const { Api } = require("telegram");

/**
 * Get a message from a Telegram channel.
 */
async function getMessage(client, channelId, messageId) {
    const { Api } = require("telegram");

async function getMessage(client, channelId, messageId) {
    try {
        const entity = await client.getEntity(channelId);

        const messages = await client.getMessages(entity, {
            ids: [Number(messageId)]
        });

        return messages?.[0] || null;
    } catch (err) {
        console.error("Telegram getMessage failed:", err);
        return null;
    }
}

function getVideoMedia(message) {
    if (!message || !message.media) return null;

    if (message.media instanceof Api.MessageMediaDocument) {
        return message.media.document;
    }

    return null;
}

module.exports = {
    getMessage,
    getVideoMedia
};
}

/**
 * Check if message contains a video.
 */
function getVideoMedia(message) {
    if (!message || !message.media) return null;

    if (message.media instanceof Api.MessageMediaDocument) {
        return message.media.document;
    }

    return null;
}

module.exports = {
    getMessage,
    getVideoMedia
};
