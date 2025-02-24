const { Queue } = require("bullmq"); 
const { createClient } = require("redis");

// ✅ Setup Redis Connection
const connection = {
  host: process.env.REDIS_HOST || "127.0.0.1",
  port: process.env.REDIS_PORT || 6379,
};

// ✅ Initialize Redis Client
const redisClient = createClient({ socket: connection });

redisClient.on("error", (err) => console.error("❌ Redis Client Error:", err));
redisClient.connect().then(() => console.log("✅ Redis connected successfully."));

// ✅ Initialize BullMQ Queue (batchQueue)
const batchQueue = new Queue("batchQueue", {
  connection,
  defaultJobOptions: {
    removeOnComplete: 100, 
    removeOnFail: 50, 
    attempts: 3, 
    backoff: { type: "exponential", delay: 5000 }, 
  },
});

// ✅ Log when Redis is connected
batchQueue.client.then(() => console.log("✅ BullMQ batchQueue connected to Redis"));

module.exports = {
  batchQueue,
  redisClient,
};
