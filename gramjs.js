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
 * Telegram stores each file's bytes on exactly one data center. When the
 * home client (connected to its own "home" DC) asks for a file that
 * actually lives elsewhere, upload.GetFile fails with
 * "FILE_MIGRATE_<dcId>" (or NETWORK_MIGRATE_<dcId>). The fix documented by
 * Telegram itself (https://core.telegram.org/api/files#downloading-files)
 * is: create a connection to that DC, export the current authorization
 * from the home client, and import it into the new connection - the two
 * connections then share the same authorized user/bot, just on different
 * data centers.
 *
 * This module keeps a small pool of already-authorized TelegramClient
 * instances, one per DC, built entirely from PUBLIC GramJS/MTProto calls
 * (help.GetConfig, auth.exportAuthorization, auth.importAuthorization, and
 * the public Session#setDC method) - no private/underscore GramJS
 * internals are used.
 * ---------------------------------------------------------------------
 */

/** @type {Map<number, import('telegram').TelegramClient>} dcId -> authorized client */
const dcClients = new Map();
/** @type {Map<number, Promise<import('telegram').TelegramClient>>} dcId -> in-flight creation, so concurrent streams don't each build their own client for the same DC */
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

/**
 * Fetches Telegram's current DC address list via the PUBLIC help.getConfig
 * RPC (not a hardcoded IP list, and not a private GramJS internal) and
 * picks the best IPv4, non-CDN option for the requested dcId.
 */
async function resolveDcOption(client, dcId) {
  const config = await client.invoke(new Api.help.GetConfig());
  const options = (config.dcOptions || []).filter((o) => o.id === dcId && !o.cdn);

  // Prefer a plain IPv4, non-media-only address; fall back to whatever's
  // available for this DC if nothing matches that exact preference.
  const best =
    options.find((o) => !o.ipv6 && !o.mediaOnly) ||
    options.find((o) => !o.ipv6) ||
    options[0];

  if (!best) {
    throw new Error(`No DC address found for DC ${dcId} in Telegram's config.`);
  }

  return { ipAddress: best.ipAddress, port: best.port };
}

/**
 * Builds and authorizes a brand-new TelegramClient connected directly to
 * `dcId`, sharing the same account as `primaryClient` via
 * export/import authorization (steps 4 and 5 of the migration flow).
 */
async function createClientForDC(dcId) {
  console.log(`[MTProto] Creating new client for DC ${dcId}...`);

  const { ipAddress, port } = await resolveDcOption(primaryClient, dcId);

  const session = new StringSession(""); // fresh session - it will get its own auth key for this DC
  const newClient = new TelegramClient(session, apiId, apiHash, baseClientOptions());

  // Point this client's session at the target DC's address BEFORE
  // connecting, so its MTProto handshake happens against DC `dcId`
  // directly rather than the default/home DC.
  newClient.session.setDC(dcId, ipAddress, port);

  await newClient.connect();

  // Step 4: export authorization from the current (home) client for the
  // target DC.
  const exported = await primaryClient.invoke(
    new Api.auth.ExportAuthorization({ dcId })
  );

  // Step 5: import that authorization into the new connection, binding it
  // to the same authorized bot/user account.
  await newClient.invoke(
    new Api.auth.ImportAuthorization({ id: exported.id, bytes: exported.bytes })
  );

  console.log(`[MTProto] Client for DC ${dcId} authorized and cached.`);
  return newClient;
}

/**
 * Returns an authorized TelegramClient for `dcId`, reusing a cached one if
 * we already have it (step 3/9: reuse-or-create, cache one client per DC),
 * creating (and caching) a new one otherwise. Concurrent requests for the
 * same not-yet-cached DC share a single in-flight creation instead of each
 * spinning up their own client.
 */
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
