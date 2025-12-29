const express = require("express");
const fs = require("fs");

const { withRedis } = require("../services/redis");
const { readMappings, writeMappings } = require("../services/mappings-store");
const { clearLogs } = require("../services/log-files");
const {
  restartWorkers,
  stopWorkersIgnoreErrors,
  startWorkersIgnoreErrors,
} = require("../services/pm2");

function createAdminRouter(config) {
  const router = express.Router();

  router.post("/restart-workers", async (req, res) => {
    try {
      console.log(`[admin] [${config.envLabel}] Restarting workers...`);
      await restartWorkers();
      res.json({
        success: true,
        message: "Workers restarted",
        environment: config.envLabel,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error(`[admin] Restart error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/flush-redis", async (req, res) => {
    try {
      console.log(`[admin] [${config.envLabel}] Flushing Redis...`);

      await withRedis(config.redisUrl, async (redis) => {
        await redis.flushDb();
      });

      console.log(`[admin] [${config.envLabel}] Redis flushed`);

      res.json({
        success: true,
        message: "Redis flushed",
        environment: config.envLabel,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error(`[admin] Flush error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/full-reset", async (req, res) => {
    try {
      console.log(`[admin] [${config.envLabel}] Full reset initiated...`);

      // Step 1: Stop workers
      await stopWorkersIgnoreErrors();

      // Step 2: Flush Redis
      await withRedis(config.redisUrl, async (redis) => {
        await redis.flushDb();
      });

      // Step 3: Clear checkpoint file(s)
      for (const checkpointPath of config.paths.checkpointPaths) {
        if (fs.existsSync(checkpointPath)) {
          fs.unlinkSync(checkpointPath);
        }
      }

      // Step 4: Reset file statuses in mappings
      const mappings = readMappings(config.paths.mappingsPath);
      mappings.files.forEach((file) => {
        if (file.status === "processing") file.status = "ready";
      });
      writeMappings(config.paths.mappingsPath, mappings);

      // Step 5: Start workers
      startWorkersIgnoreErrors();

      console.log(`[admin] [${config.envLabel}] Full reset completed`);

      res.json({
        success: true,
        message: "Full reset completed. Workers restarting.",
        environment: config.envLabel,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error(`[admin] Full reset error: ${err.message}`);
      startWorkersIgnoreErrors();
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/clear-logs", async (req, res) => {
    try {
      const envOnly = req.body?.envOnly || false;

      console.log(
        `[admin] [${config.envLabel}] Clearing log files (envOnly: ${envOnly})...`
      );

      const result = clearLogs({
        outputDir: config.paths.outputDir,
        envLabel: config.envLabel,
        envOnly,
      });

      console.log(`[admin] [${config.envLabel}] Log files cleared`);

      res.json({
        success: true,
        message: result.message,
        environment: config.envLabel,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error(`[admin] Clear logs error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}

module.exports = {
  createAdminRouter,
};
