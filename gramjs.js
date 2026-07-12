require("dotenv").config();

const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");

const apiId = Number(process.env.API_ID);
const apiHash = process.env.API_HASH;
const stringSession = new StringSession(process.env.STRING_SESSION || "");

async function startClient() {
  if (!process.env.STRING_SESSION) {
    throw new Error("STRING_SESSION is missing in .env");
  }

  const client = new TelegramClient(
    stringSession,
    apiId,
    apiHash,
    {
      connectionRetries: 5,
      retryDelay: 2000,
      timeout: 30,
      requestRetries: 5,
      downloadRetries: 5,
      autoReconnect: true,
      floodSleepThreshold: 60,
    }
  );

  console.log("Connecting to Telegram...");

  await client.connect();

  const authorized = await client.isUserAuthorized();

  if (!authorized) {
    throw new Error("Invalid STRING_SESSION. Generate a new session.");
  }

  console.log("✅ Telegram MTProto connected!");
  console.log(`Logged in as: ${(await client.getMe()).username || (await client.getMe()).firstName}`);
  console.log(`Main DC: ${client.session.dcId}`);

  return client;
}

module.exports = {
  startClient,
};

if (require.main === module) {
  startClient()
    .then(() => {
      console.log("Login successful");
      process.exit(0);
    })
    .catch((err) => {
      console.error(err);
      process.exit(1);
    });
}
