/**
 * index.js
 * Main application entry point - Job creation and queue management
 * 
 * ENHANCEMENTS:
 * - Bull Board enabled for ALL environments (including production)
 * - Basic auth protection for Bull Board in production
 * - Environment-aware logging
 */

const dotenv = require("dotenv");
dotenv.config();

const express = require("express");
const { performance } = require("perf_hooks");

const { appRedis, batchQueue } = require("./src/services/queue");
const { processReadyCsvFilesFromMappings } = require("./src/services/s3-helpers");
const {
  logger,
  logErrorToFile,
  logUpdatesToFile,
  logInfoToFile,
  logProgressToFile,
  ENV_LABEL,
} = require("./src/utils/logger");
const {
  resolveAppEnv,
  getS3BucketName,
  requireNonEmpty,
} = require("./src/config/runtime-env");
const { createUniqueJobId } = require("./src/utils/utils");
const { addBatchJob } = require("./src/services/job-manager");

const { BullMQAdapter } = require("@bull-board/api/bullMQAdapter");
const { createBullBoard } = require("@bull-board/api");
const { ExpressAdapter } = require("@bull-board/express");

// =============================================================================
// EXPRESS SETUP
// =============================================================================

const app = express();
app.use(express.json());

// =============================================================================
// ENVIRONMENT CONFIGURATION
// =============================================================================

const appEnv = resolveAppEnv(process.env);
logInfoToFile(`🚀 Starting application in ${appEnv} environment`);

// =============================================================================
// BASIC AUTH MIDDLEWARE FOR PRODUCTION
// =============================================================================

/**
 * Basic authentication middleware for Bull Board in production
 * Uses environment variables for credentials
 */
const basicAuthMiddleware = (req, res, next) => {
  // Skip auth for non-production environments
  if (appEnv !== "production") {
    return next();
  }
  
  // Get credentials from environment or use defaults
  const ADMIN_USER = process.env.BULL_BOARD_USER || "admin";
  const ADMIN_PASS = process.env.BULL_BOARD_PASS || "woo-update-2024";
  
  const authHeader = req.headers.authorization;
  
  if (!authHeader) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Bull Board Admin"');
    return res.status(401).send("Authentication required");
  }
  
  const [type, credentials] = authHeader.split(" ");
  
  if (type !== "Basic" || !credentials) {
    res.setHeader("WWW-Authenticate", 'Basic realm="Bull Board Admin"');
    return res.status(401).send("Invalid authentication");
  }
  
  const decoded = Buffer.from(credentials, "base64").toString("utf-8");
  const [user, pass] = decoded.split(":");
  
  if (user === ADMIN_USER && pass === ADMIN_PASS) {
    return next();
  }
  
  res.setHeader("WWW-Authenticate", 'Basic realm="Bull Board Admin"');
  return res.status(401).send("Invalid credentials");
};

// =============================================================================
// BULL BOARD SETUP (ALL ENVIRONMENTS)
// =============================================================================

const serverAdapter = new ExpressAdapter();
serverAdapter.setBasePath("/admin/queues");

// Apply basic auth for production, open access for dev/staging
app.use("/admin/queues", basicAuthMiddleware, serverAdapter.getRouter());

createBullBoard({
  queues: [new BullMQAdapter(batchQueue)],
  serverAdapter: serverAdapter,
});

if (appEnv === "production") {
  logInfoToFile("✅ Bull Board initialized with basic auth protection");
} else {
  logInfoToFile("✅ Bull Board initialized (open access for dev/staging)");
}

// =============================================================================
// ENVIRONMENT INFO ENDPOINT
// =============================================================================

/**
 * GET /api/environment
 * Returns current environment information for the UI
 */
app.get("/api/environment", (req, res) => {
  res.json({
    mode: appEnv,
    label: ENV_LABEL,
    bucket: getS3BucketName(process.env, appEnv),
    timestamp: new Date().toISOString(),
  });
});

// =============================================================================
// TIMING & ERROR HANDLING
// =============================================================================

const startTime = performance.now();

/**
 * Unified error handler that logs duration before failing
 */
const handleProcessError = (error, type = "Error") => {
  const duration = ((performance.now() - startTime) / 1000).toFixed(2);
  logger.error(`${type} occurred after ${duration} seconds:`, error);
  process.exit(1);
};

// =============================================================================
// MAIN PROCESS
// =============================================================================

/**
 * Main process: Read CSV mappings and enqueue jobs
 */
const mainProcess = async () => {
  try {
    const s3BucketName = getS3BucketName(process.env, appEnv);

    // Fail fast with clear messaging (Option A + legacy fallback supported).
    requireNonEmpty(
      s3BucketName,
      "S3_BUCKET_NAME_PRODUCTION|S3_BUCKET_NAME_STAGING|S3_BUCKET_NAME_DEVELOPMENT (or legacy S3_BUCKET_NAME/S3_BUCKET_NAME_TEST)"
    );

    logInfoToFile(`📦 Using S3 bucket: ${s3BucketName}`);

    const batchSize = parseInt(process.env.BATCH_SIZE) || 20;
    logInfoToFile(`📊 Batch size: ${batchSize}`);

    // Process CSV files that are marked as "ready"
    await processReadyCsvFilesFromMappings(s3BucketName, batchSize);

    logInfoToFile("✅ Job enqueuing complete. Workers will process the queue.");
    
  } catch (error) {
    handleProcessError(error, "Main process error");
  }
};

// =============================================================================
// GRACEFUL SHUTDOWN
// =============================================================================

const gracefulShutdown = async (signal) => {
  logInfoToFile(`${signal} received. Gracefully shutting down...`);
  
  try {
    await appRedis.quit();
    logInfoToFile("Redis connection closed.");
  } catch (error) {
    logErrorToFile(`Error during shutdown: ${error.message}`);
  }
  
  process.exit(0);
};

process.on("SIGTERM", () => gracefulShutdown("SIGTERM"));
process.on("SIGINT", () => gracefulShutdown("SIGINT"));
process.on("uncaughtException", (error) => handleProcessError(error, "Uncaught Exception"));
process.on("unhandledRejection", (error) => handleProcessError(error, "Unhandled Rejection"));

// =============================================================================
// START SERVER
// =============================================================================

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
  logInfoToFile(`🌐 Express server running on port ${PORT}`);
  logInfoToFile(`📊 Bull Board: http://localhost:${PORT}/admin/queues`);
  
  // Run main process after server starts
  mainProcess();
});