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
    }
  );

  await client.start({
    phoneNumber: async () => await input.text("Phone Number: "),
    password: async () => await input.text("2FA Password (if any): "),
    phoneCode: async () => await input.text("Telegram Code: "),
    onError: (err) => console.log(err),
  });

  
console.log("✅ Telegram MTProto connected!");
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
