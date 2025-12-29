/**
 * logger.js
 * Enhanced logging system with environment-aware prefixes
 * 
 * FEATURES:
 * - Environment prefix on every log line: [PROD], [STAGING], [DEV]
 * - Log rotation at 5MB with archived file naming
 * - PST timestamps for consistency
 * - Redis-based progress tracking
 * - Prevents duplicate completion logging
 */

const fs = require("fs");
const path = require("path");
const pino = require("pino");
const pinoPretty = require("pino-pretty");
const dayjs = require('dayjs');
const utc = require('dayjs/plugin/utc');
const timezone = require('dayjs/plugin/timezone');
const { appRedis } = require('../services/queue');

// Track files already logged as complete (prevents spam)
const completedFilesLogged = new Set();

// Extend dayjs with UTC and timezone plugins
dayjs.extend(utc);
dayjs.extend(timezone);

// =============================================================================
// ENVIRONMENT CONFIGURATION
// =============================================================================

/**
 * Get the current execution mode and derive environment label
 */
const executionMode = process.env.EXECUTION_MODE || "production";

/**
 * Environment label for log prefixes
 * Maps EXECUTION_MODE to human-readable labels
 */
const getEnvLabel = () => {
  switch (executionMode) {
    case "production":
      return "PROD";
    case "test":
      return "STAGING";
    case "development":
      return "DEV";
    default:
      return executionMode.toUpperCase();
  }
};

const ENV_LABEL = getEnvLabel();

// =============================================================================
// FILE PATHS
// =============================================================================

// NOTE: logger.js lives in src/utils; output-files is at repo root.
// src/utils -> src -> repo root
const progressFilePath = path.join(__dirname, "..", "..", "output-files", "update-progress.txt");
const errorFilePath = path.join(__dirname, "..", "..", "output-files", "error-log.txt");
const infoFilePath = path.join(__dirname, "..", "..", "output-files", "info-log.txt");
const updatesFilePath = path.join(__dirname, "..", "..", "output-files", "updates-log.txt");

// =============================================================================
// LOG ROTATION CONFIGURATION
// =============================================================================

// Log file size limit (5 MB)
const maxSize = 5 * 1024 * 1024; // 5 MB

// Maximum number of archived files to keep per log type
const maxArchivedFiles = 10;

// =============================================================================
// UTILITY FUNCTIONS
// =============================================================================

/**
 * Get PST timestamp for log entries
 */
const getPSTTimestamp = () => dayjs().tz("America/Los_Angeles").format("YYYY-MM-DD HH:mm:ss");

/**
 * Get safe PST timestamp for filenames (no colons)
 */
const getSafePSTTimestamp = () => dayjs().tz("America/Los_Angeles").format("YYYY-MM-DD_HH-mm-ss");

/**
 * Ensure directory exists for a file path
 */
const ensureDirectoryExists = (filePath) => {
  const dir = path.dirname(filePath);
  if (!fs.existsSync(dir)) {
    fs.mkdirSync(dir, { recursive: true });
  }
};

/**
 * Clean up old archived log files, keeping only the most recent ones
 * @param {string} filePath - The main log file path
 */
const cleanupOldArchives = (filePath) => {
  try {
    const dir = path.dirname(filePath);
    const baseName = path.basename(filePath, '.txt');
    
    // Find all archived files for this log type
    const files = fs.readdirSync(dir)
      .filter(f => f.startsWith(baseName) && f.includes('-archived-'))
      .map(f => ({
        name: f,
        path: path.join(dir, f),
        mtime: fs.statSync(path.join(dir, f)).mtime.getTime()
      }))
      .sort((a, b) => b.mtime - a.mtime); // Newest first
    
    // Delete files beyond the max limit
    if (files.length > maxArchivedFiles) {
      const toDelete = files.slice(maxArchivedFiles);
      toDelete.forEach(file => {
        fs.unlinkSync(file.path);
        console.log(`[${getPSTTimestamp()}] [${ENV_LABEL}] Cleaned up old archive: ${file.name}`);
      });
    }
  } catch (error) {
    console.error(`[${getPSTTimestamp()}] [${ENV_LABEL}] Failed to cleanup archives: ${error.message}`);
  }
};

/**
 * Rotate log files if size exceeds the limit
 * Renames current file to archived format and cleans up old archives
 */
const rotateLogFile = (filePath) => {
  try {
    if (fs.existsSync(filePath) && fs.statSync(filePath).size > maxSize) {
      const rotatedFileName = filePath.replace(".txt", `-archived-${getSafePSTTimestamp()}.txt`);
      fs.renameSync(filePath, rotatedFileName);
      console.log(`[${getPSTTimestamp()}] [${ENV_LABEL}] Rotated log file: ${rotatedFileName}`);
      
      // Clean up old archives
      cleanupOldArchives(filePath);
    }
  } catch (error) {
    console.error(`[${getPSTTimestamp()}] [${ENV_LABEL}] Failed to rotate log file: ${error.message}`);
  }
};

/**
 * Write content to log files with rotation
 */
const writeToFile = (filePath, content) => {
  try {
    ensureDirectoryExists(filePath);
    rotateLogFile(filePath);
    fs.appendFileSync(filePath, content, "utf-8");
  } catch (error) {
    console.error(`[${getPSTTimestamp()}] [${ENV_LABEL}] Failed to write to file: ${filePath}. Error: ${error.message}`);
  }
};

/**
 * Write the progress log file
 */
const writeProgressToFile = (content) => {
  writeToFile(progressFilePath, content);
};

// =============================================================================
// LOGGING FUNCTIONS WITH ENVIRONMENT PREFIXES
// =============================================================================

/**
 * Log error messages to error-log.txt
 * @param {string} message - Error message
 * @param {Error|null} error - Optional error object for stack trace
 */
const logErrorToFile = (message, error = null) => {
  let errorContent = `[${getPSTTimestamp()}] [${ENV_LABEL}] ${message}\n`;
  if (error && error.stack) {
    errorContent += `Stack Trace:\n${error.stack}\n`;
  }
  writeToFile(errorFilePath, errorContent);
};

/**
 * Log info messages to info-log.txt
 * @param {string} message - Info message
 */
const logInfoToFile = (message) => {
  const content = `[${getPSTTimestamp()}] [${ENV_LABEL}] ${message}\n`;
  writeToFile(infoFilePath, content);
};

/**
 * Log update messages to updates-log.txt
 * @param {string} message - Update message
 */
const logUpdatesToFile = (message) => {
  const content = `[${getPSTTimestamp()}] [${ENV_LABEL}] ${message}\n`;
  writeToFile(updatesFilePath, content);
};

/**
 * Fetch progress from Redis and log to file
 * @returns {Promise<boolean>} True if all files are complete
 */
const logProgressToFile = async () => {
  try {
    const fileKeys = await appRedis.keys("total-rows:*");
  
    if (fileKeys.length === 0) {
      console.log(`[${getPSTTimestamp()}] [${ENV_LABEL}] No progress to log.`);
      return false;
    }

    let progressLogs = "";
    let allFilesComplete = true;

    for (const key of fileKeys) {
      // Robust fileKey extraction (handles colons in filename)
      const fileKey = key.replace(/^total-rows:/, "");

      const totalRows = parseInt(await appRedis.get(`total-rows:${fileKey}`) || 0, 10);
      const updated = parseInt(await appRedis.get(`updated-products:${fileKey}`) || 0, 10);
      const skipped = parseInt(await appRedis.get(`skipped-products:${fileKey}`) || 0, 10);
      const failed = parseInt(await appRedis.get(`failed-products:${fileKey}`) || 0, 10);

      const completed = updated + skipped + failed;
      const progress = totalRows > 0 ? Math.round((completed / totalRows) * 100) : 0;
      const isComplete = completed >= totalRows && totalRows > 0;

      // Skip spam for completed files - log completion ONCE only
      if (isComplete) {
        if (!completedFilesLogged.has(fileKey)) {
          progressLogs += `[${getPSTTimestamp()}] [${ENV_LABEL}] ✅ File COMPLETED: ${fileKey}\n`;
          progressLogs += `   Final stats - Updated: ${updated}, Skipped: ${skipped}, Failed: ${failed}, Total: ${totalRows}\n\n`;
          completedFilesLogged.add(fileKey);
        }
        continue;
      }

      // File is still processing
      allFilesComplete = false;

      progressLogs += `[${getPSTTimestamp()}] [${ENV_LABEL}] 📊 File ${fileKey}: ${completed}/${totalRows} rows processed (${progress}%)\n`;
      progressLogs += `   Updated: ${updated}, Skipped: ${skipped}, Failed: ${failed}\n\n`;
    }

    if (progressLogs.trim()) {
      writeProgressToFile(progressLogs);
    }

    return allFilesComplete;
    
  } catch (error) {
    logErrorToFile(`Error logging progress: ${error.message}`);
    return false;
  }
};

// =============================================================================
// PINO LOGGER SETUP
// =============================================================================

const pinoLogger = pino(
  {
    base: null,
    timestamp: () => `,"time":"${getPSTTimestamp()}","env":"${ENV_LABEL}"`,
  },
  pinoPretty({
    levelFirst: true,
    colorize: true,
    translateTime: false,
    messageFormat: `[${ENV_LABEL}] {msg}`,
  })
);

// =============================================================================
// EXPORTS
// =============================================================================

module.exports = {
  logger: pinoLogger,
  logUpdatesToFile,
  logErrorToFile,
  logInfoToFile,
  logProgressToFile,
  // Export for external use
  ENV_LABEL,
  executionMode,
  getPSTTimestamp,
};