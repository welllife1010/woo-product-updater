const { createClient } = require("redis");

async function withRedis(redisUrl, fn) {
  const redis = createClient({ url: redisUrl });
  await redis.connect();
  try {
    return await fn(redis);
  } finally {
    try {
      await redis.quit();
    } catch {
      // ignore
    }
  }
}

module.exports = {
  withRedis,
};
