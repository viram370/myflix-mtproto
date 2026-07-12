require("dotenv").config();

const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const input = require("input");

const apiId = Number(process.env.API_ID);
const apiHash = process.env.API_HASH;

// Leave empty the first time.
// After login, replace this with the saved session string.
const stringSession = new StringSession(process.env.STRING_SESSION || "");

async function startClient() {
  const client = new TelegramClient(
    stringSession,
    apiId,
    apiHash,
    {
      connectionRetries: 5,
      // The stream route's diagnostics point at a stall in
      // establishing/exporting a sender for a video's DC when it differs
      // from this client's main DC (cross-DC file downloads need their
      // own authorized connection to that DC). GramJS's defaults for the
      // options below are tuned for interactive bot/userbot usage, not for
      // a server that needs to reliably open fresh cross-DC download
      // connections on demand - widen them so a slow (but eventually
      // successful) DC handshake has room to complete instead of being
      // abandoned/retried too aggressively:
      retryDelay: 2000, // ms to wait between connection retries
      timeout: 30, // seconds - per-request timeout used by the MTProto sender
      requestRetries: 5, // retry individual RPCs (e.g. upload.getFile) this many times
      downloadRetries: 5, // retry download-specific RPCs this many times
      autoReconnect: true,
      floodSleepThreshold: 60, // auto-sleep through short FLOOD_WAITs instead of throwing
    }
  );

  await client.start({
    phoneNumber: async () => await input.text("Phone Number: "),
    password: async () => await input.text("2FA Password (if any): "),
    phoneCode: async () => await input.text("Telegram Code: "),
    onError: (err) => console.log(err),
  });

  console.log("✅ Telegram MTProto connected!");
  console.log(`[gramjs] Main session DC: ${client.session?.dcId ?? "unknown"}`);
  console.log("\n===== SAVE THIS STRING =====\n");

const fs = require("fs");
fs.writeFileSync("session.txt", client.session.save());
console.log("Session saved to session.txt");

console.log("\n============================\n");
  return client;
}

module.exports = {
  startClient,
};
if (require.main === module) {
    startClient()
        .then(() => console.log("Login completed"))
        .catch(console.error);
}
