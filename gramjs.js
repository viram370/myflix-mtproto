require("dotenv").config();

const { TelegramClient } = require("telegram");
const { StringSession } = require("telegram/sessions");
const { Api } = require("telegram");

const apiId = Number(process.env.API_ID);
const apiHash = process.env.API_HASH;
const stringSession = new StringSession(process.env.STRING_SESSION || "");

/**
 * ---------------------------------------------------------------------
 * PER-DC TELEGRAM CLIENT POOL
 * ---------------------------------------------------------------------
 */

const dcClients = new Map();
const dcClientCreationInFlight = new Map();

let primaryClient = null;
let primaryDcId = null;

function baseClientOptions() {
  return {
    connectionRetries: 5,
    retryDelay: 2000,
    timeout: 30,
    requestRetries: 5,
    downloadRetries: 5,
    autoReconnect: true,
    floodSleepThreshold: 60,
  };
}

async function startClient() {
  if (!process.env.STRING_SESSION) {
    throw new Error("STRING_SESSION is missing in .env");
  }

  const client = new TelegramClient(stringSession, apiId, apiHash, baseClientOptions());

  console.log("Connecting to Telegram...");

  await client.connect();

  const authorized = await client.isUserAuthorized();

  if (!authorized) {
    throw new Error("Invalid STRING_SESSION. Generate a new session.");
  }

  console.log("✅ Telegram MTProto connected!");
  console.log(`Logged in as: ${(await client.getMe()).username || (await client.getMe()).firstName}`);
  console.log(`Main DC: ${client.session.dcId}`);

  primaryClient = client;
  primaryDcId = client.session.dcId;
  dcClients.set(primaryDcId, client);

  return client;
}

async function resolveDcOption(client, dcId) {
  const config = await client.invoke(new Api.help.GetConfig());
  const options = (config.dcOptions || []).filter((o) => o.id === dcId && !o.cdn);

  const best =
    options.find((o) => !o.ipv6 && !o.mediaOnly) ||
    options.find((o) => !o.ipv6) ||
    options[0];

  if (!best) {
    throw new Error(`No DC address found for DC ${dcId} in Telegram's config.`);
  }

  return { ipAddress: best.ipAddress, port: best.port };
}

async function createClientForDC(dcId) {
  console.log(`[MTProto] Connecting to DC ${dcId}...`);

  const { ipAddress, port } = await resolveDcOption(primaryClient, dcId);

  const session = new StringSession(""); 
  const newClient = new TelegramClient(session, apiId, apiHash, baseClientOptions());

  newClient.session.setDC(dcId, ipAddress, port);

  await newClient.connect();

  console.log("[MTProto] Exporting authorization...");
  const exported = await primaryClient.invoke(
    new Api.auth.ExportAuthorization({ dcId })
  );

  console.log("[MTProto] Importing authorization...");
  await newClient.invoke(
    new Api.auth.ImportAuthorization({ id: exported.id, bytes: exported.bytes })
  );

  console.log("[MTProto] Migration successful.");
  return newClient;
}

async function getClientForDC(dcId) {
  if (!primaryClient) {
    throw new Error("Primary Telegram client is not started yet.");
  }

  if (dcId === primaryDcId) {
    return primaryClient;
  }

  const cached = dcClients.get(dcId);
  if (cached) {
    if (cached.connected) return cached;
    try {
      await cached.connect();
      return cached;
    } catch (err) {
      console.warn(`[MTProto] Cached client for DC ${dcId} failed to reconnect (${err.message}) - recreating.`);
      dcClients.delete(dcId);
    }
  }

  const inFlight = dcClientCreationInFlight.get(dcId);
  if (inFlight) return inFlight;

  const creation = createClientForDC(dcId)
    .then((client) => {
      dcClients.set(dcId, client);
      return client;
    })
    .finally(() => {
      dcClientCreationInFlight.delete(dcId);
    });

  dcClientCreationInFlight.set(dcId, creation);
  return creation;
}

function getPrimaryDcId() {
  return primaryDcId;
}

module.exports = {
  startClient,
  getClientForDC,
  getPrimaryDcId,
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
