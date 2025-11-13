/**
 * queue.js - BullMQ queue + app Redis client (env-driven)
 *
 * BullMQ uses ioredis under the hood (we pass "connection" options).
 * Your app uses node-redis for simple KV (counters, checkpoints, etc.).
 *
 * IMPORTANT:
 * - In normal runs (production / dev), we connect to Redis.
 * - In Jest tests (NODE_ENV === "test"), we DO NOT connect to Redis
 *   and DO NOT create real BullMQ Queue connections.
 *   Instead, we export lightweight "fake" objects so tests can import
 *   this module without requiring a live Redis server.
 */


const { Queue, QueueEvents } = require("bullmq");
const { createClient } = require("redis");

// ---------------------------------------------
// 0) Detect environment (normal vs Jest tests)
// ---------------------------------------------

/**
 * When you run `npm test`, our package.json sets:
 *   "test": "NODE_ENV=test jest"
 *
 * So inside tests, process.env.NODE_ENV === "test".
 * We use this to avoid connecting to Redis during unit tests.
 */
const isTestEnv = process.env.NODE_ENV === "test";

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

/**
 * This connection object is used by BullMQ (which uses ioredis).
 * It is a lower-level Redis connection specifically for queues, jobs, etc.
 */
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
// 3) App Redis (node-redis) for our own KV storage
// -------------------------------------------------

/**
 * This client is for our app's key/value needs:
 * - counters
 * - checkpoints
 * - progress tracking
 *
 * We build a URL and pass it to node-redis' createClient().
 */

// Prefer a single URL when available (especially for cloud/TLS).
let appRedisUrl = REDIS_URL_APP;
if (!appRedisUrl) {
  // Build a URL from host/port/db; include auth + TLS only if envs are set.
  const scheme = USE_TLS ? "rediss" : "redis";
  const authPart = REDIS_PASSWORD
    ? `${REDIS_USERNAME ? `${encodeURIComponent(REDIS_USERNAME)}:` : ""}${encodeURIComponent(
        REDIS_PASSWORD
      )}@`
    : "";
  appRedisUrl = `${scheme}://${authPart}${REDIS_HOST}:${REDIS_PORT}/${REDIS_DB_APP}`;
}

/**
 * appRedis is a node-redis client.
 * NOTE: Creating the client does NOT connect yet.
 * We call .connect() later (except in tests).
 */
const appRedis = createClient({ url: appRedisUrl });

// Helpful connection logs (these just listen; they do not connect on their own).
appRedis.on("error", (err) => console.error("[appRedis] ❌", err));
appRedis.on("connect", () => console.log("[appRedis] ⚙️  Connecting..."));
appRedis.on("ready", () =>
  console.log("[appRedis] ✅ Ready:", appRedisUrl)
);
appRedis.on("end", () => console.log("[appRedis] ⛔ Disconnected"));

/**
 * Connect right away in normal runs.
 * In Jest tests (NODE_ENV === "test"), we SKIP the connect().
 *
 * This is the key to avoid "ECONNREFUSED 127.0.0.1:6379"
 * when you run unit tests without a Redis server.
 */
if (!isTestEnv) {
  appRedis.connect().catch((err) => {
    console.error("[appRedis] ❌ Failed to connect:", err);
  });
} else {
  // In test mode, you can still call appRedis methods if you mock them,
  // but no real network connection is attempted.
  console.log("[appRedis] 🧪 Test mode: skipping Redis connect()");
}

// ---------------------------------------------
// 4) BullMQ queue + (optional) queue events
// ---------------------------------------------

let batchQueue;
let batchQueueEvents;

/**
 * batchQueue is the main BullMQ queue used by our jobs.
 * Constructing a Queue does not immediately spam logs by itself.
 * However, accessing batchQueue.client WILL cause a Redis connection.
 */
if (!isTestEnv) {
  /**
   * REAL BullMQ queue (non-test).
   * This will create a real Redis connection via ioredis.
   */
  batchQueue = new Queue(QUEUE_NAME, {
    connection: bullmqConnection,
    prefix: BULLMQ_PREFIX, // keys: <prefix>:<queueName>:<suffix>
    defaultJobOptions: {
      removeOnComplete: 100,
      removeOnFail: 50,
      attempts: 3,
      backoff: { type: "exponential", delay: 5000 },
    },
  });

  /**
   * REAL QueueEvents (non-test) – to listen for "completed", "failed", etc.
   */
  batchQueueEvents = new QueueEvents(QUEUE_NAME, {
    connection: bullmqConnection,
  });

  // Log BullMQ connection – this touches batchQueue.client and connects.
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
} else {
  /**
   * TEST MODE: Fake in-memory "queue" object.
   *
   * Your code that uses batchQueue probably calls methods like:
   *   - batchQueue.add(name, data, opts?)
   *   - batchQueue.getJobs([...states])
   *
   * We provide no-op / fake versions here so tests can run without Redis.
   */
  batchQueue = {
    /**
     * Simulate adding a job. We just return a fake job object.
     * This keeps job-enqueue code from crashing in tests.
     */
    add: async (name, data, opts) => {
      return {
        id: "fake-job-id",
        name,
        data,
        opts,
      };
    },

    /**
     * Simulate fetching jobs by states (e.g. ["waiting", "active"]).
     * In tests, we default to "no jobs" unless you override/mutate this.
     */
    getJobs: async (/* states */) => {
      return [];
    },

    /**
     * Allow code to call batchQueue.close() in tests.
     */
    close: async () => {},

    /**
     * Optional: if any code touches batchQueue.client in tests,
     * provide a resolved promise to avoid "undefined" errors.
     */
    client: Promise.resolve(),
  };

  /**
   * TEST MODE: Fake QueueEvents – no real Redis connection.
   */
  batchQueueEvents = {
    /**
     * Allow code to register handlers:
     *   batchQueueEvents.on('completed', handler);
     * We simply ignore them in tests.
     */
    on: () => {},

    /**
     * Allow graceful shutdown calls:
     *   await batchQueueEvents.close();
     */
    close: async () => {},
  };

  console.log("[BullMQ] 🧪 Test mode: using fake batchQueue & QueueEvents (no Redis connections)");
}
// ---------------------------------------------
// 5) Small helpers (namespacing our keys)
// ---------------------------------------------

/**
 * @function appKey
 * @description Prefixes our application keys so all our Redis entries
 * live under a consistent namespace, e.g.:
 *   appKey("total-rows:filename.csv")
 * becomes:
 *   "woo_updater:total-rows:filename.csv"
 */
function appKey(shortKey) {
  return `${APP_KEY_PREFIX}${shortKey}`;
}

module.exports = {
  // Job system
  batchQueue,
  batchQueueEvents,
  bullmqConnection,

  // Our KV client + key helpers
  appRedis,
  APP_KEY_PREFIX,
  appKey,
};
