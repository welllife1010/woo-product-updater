/**
 * queue.js - BullMQ queue + app Redis client (env-driven)
 *
 * BullMQ uses ioredis under the hood (we pass "connection" options).
 * Your app uses node-redis for simple KV (counters, checkpoints, etc.).
 *
 * If TLS/auth/URL envs are not provided, they are simply not used.
 */

const { Queue, QueueEvents } = require("bullmq");
const { createClient } = require("redis");

// ---------------------------
// 1) Read envs (+ defaults)
// ---------------------------
const {
  REDIS_HOST = "127.0.0.1",
  REDIS_PORT = "6379",
  REDIS_DB_BULLMQ = "0",
  REDIS_DB_APP = "1",
  REDIS_TLS = "false",
  REDIS_USERNAME,
  REDIS_PASSWORD,
  REDIS_URL_APP,           // optional full URL for app Redis client
  BULLMQ_PREFIX = "bull",
  APP_KEY_PREFIX = "woo_updater:",
  QUEUE_NAME = "batchQueue",
} = process.env;

const USE_TLS = String(REDIS_TLS).toLowerCase() === "true";

// ---------------------------------------------
// 2) BullMQ / ioredis connection (queue system)
// ---------------------------------------------
const bullmqConnection = {
  host: REDIS_HOST,
  port: Number(REDIS_PORT),
  db: Number(REDIS_DB_BULLMQ),

  // These only take effect if provided in .env
  ...(REDIS_USERNAME ? { username: REDIS_USERNAME } : {}),
  ...(REDIS_PASSWORD ? { password: REDIS_PASSWORD } : {}),
  ...(USE_TLS ? { tls: {} } : {}),
};

// -------------------------------------------------
// 3) App Redis (node-redis) for your own KV storage
// -------------------------------------------------
// Prefer a single URL when available (especially for cloud/TLS).
let appRedisUrl = REDIS_URL_APP;
if (!appRedisUrl) {
  // Build a URL from host/port/db; include auth + TLS only if envs are set.
  const scheme = USE_TLS ? "rediss" : "redis";
  const authPart = REDIS_PASSWORD
    ? `${REDIS_USERNAME ? `${encodeURIComponent(REDIS_USERNAME)}:` : ""}${encodeURIComponent(REDIS_PASSWORD)}@`
    : "";
  appRedisUrl = `${scheme}://${authPart}${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB_APP}`;
}

const appRedis = createClient({ url: appRedisUrl });

// Helpful connection logs
appRedis.on("error", (err) => console.error("[appRedis] ❌", err));
appRedis.on("connect", () => console.log("[appRedis] ⚙️  Connecting..."));
appRedis.on("ready", () =>
  console.log("[appRedis] ✅ Ready:", appRedisUrl)
);
appRedis.on("end", () => console.log("[appRedis] ⛔ Disconnected"));

// Connect right away (you can also export a connect() if you prefer)
appRedis.connect().catch((err) => {
  console.error("[appRedis] ❌ Failed to connect:", err);
});

// ---------------------------------------------
// 4) BullMQ queue + (optional) queue events
// ---------------------------------------------
const batchQueue = new Queue(QUEUE_NAME, {
  connection: bullmqConnection,
  prefix: BULLMQ_PREFIX, // keys: <prefix>:<queueName>:<suffix>
  defaultJobOptions: {
    removeOnComplete: 100,
    removeOnFail: 50,
    attempts: 3,
    backoff: { type: "exponential", delay: 5000 },
  },
});

const batchQueueEvents = new QueueEvents(QUEUE_NAME, {
  connection: bullmqConnection,
});

// Log BullMQ connection
batchQueue.client
  .then(() => {
    const tlsLabel = USE_TLS ? "TLS" : "no-TLS";
    console.log(
      `[BullMQ] ✅ '${QUEUE_NAME}' connected -> ${REDIS_HOST}:${REDIS_PORT} db=${REDIS_DB_BULLMQ} (${tlsLabel}), prefix=${BULLMQ_PREFIX}`
    );
  })
  .catch((err) => {
    console.error("[BullMQ] ❌ Failed to initialize queue client:", err);
  });

// ---------------------------------------------
// 5) Small helpers (namespacing your keys)
// ---------------------------------------------
function appKey(shortKey) {
  return `${APP_KEY_PREFIX}${shortKey}`;
}

module.exports = {
  // Job system
  batchQueue,
  batchQueueEvents,
  bullmqConnection,

  // Your KV client + key helpers
  appRedis,
  APP_KEY_PREFIX,
  appKey,
};
