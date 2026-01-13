const { Queue } = require("bullmq");

// Lazily created singleton so we don't open multiple Redis connections.
let queue = null;

function isTestEnv() {
  return process.env.NODE_ENV === "test";
}

function getBullmqConnectionFromEnv() {
  const {
    REDIS_HOST = "127.0.0.1",
    REDIS_PORT = "6379",
    REDIS_DB_BULLMQ = "0",
    REDIS_TLS = "false",
    REDIS_USERNAME,
    REDIS_PASSWORD,
  } = process.env;

  const USE_TLS = String(REDIS_TLS).toLowerCase() === "true";

  return {
    host: REDIS_HOST,
    port: Number(REDIS_PORT),
    db: Number(REDIS_DB_BULLMQ),
    ...(REDIS_USERNAME ? { username: REDIS_USERNAME } : {}),
    ...(REDIS_PASSWORD ? { password: REDIS_PASSWORD } : {}),
    ...(USE_TLS ? { tls: {} } : {}),
  };
}

function getQueueNameFromEnv() {
  return process.env.QUEUE_NAME || "batchQueue";
}

function getBullmqPrefixFromEnv() {
  return process.env.BULLMQ_PREFIX || "bull";
}

function getQueue() {
  if (queue) return queue;

  if (isTestEnv()) {
    // Avoid real Redis connections in Jest.
    queue = {
      getJobCounts: async () => ({ waiting: 0, active: 0, delayed: 0 }),
      close: async () => {},
    };
    return queue;
  }

  queue = new Queue(getQueueNameFromEnv(), {
    connection: getBullmqConnectionFromEnv(),
    prefix: getBullmqPrefixFromEnv(),
  });

  return queue;
}

async function getQueueActivityCounts() {
  const q = getQueue();
  // getJobCounts is cheap and doesn't pull job payloads.
  const counts = await q.getJobCounts("waiting", "active", "delayed");
  return {
    waiting: Number(counts.waiting) || 0,
    active: Number(counts.active) || 0,
    delayed: Number(counts.delayed) || 0,
  };
}

function isQueueRunning(counts) {
  const c = counts || {};
  return (Number(c.waiting) || 0) + (Number(c.active) || 0) + (Number(c.delayed) || 0) > 0;
}

module.exports = {
  getQueueActivityCounts,
  isQueueRunning,
};
