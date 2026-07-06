
const { Api } = require("telegram");

/**
 * Get a message from a Telegram channel.
 */
async function getMessage(client, channelId, messageId) {
    const messages = await client.getMessages(channelId, {
        ids: [Number(messageId)]
    });

    return messages[0] || null;
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
