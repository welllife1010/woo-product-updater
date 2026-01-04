const fs = require("fs");
const path = require("path");

/**
 * Get the log file path for a given type and optional environment.
 * 
 * ENVIRONMENT ISOLATION:
 * When envFilter is provided, we read from environment-specific log files
 * (e.g., info-log-production.txt, error-log-staging.txt).
 * When no envFilter, we try to read from all env-specific files and merge,
 * or fall back to legacy non-env-specific files.
 */
function getLogFilePath(outputDir, type, envFilter = null) {
  const baseName = type === "error" ? "error-log" : "info-log";
  
  if (envFilter) {
    // Map filter labels to appEnv values
    const envMap = { PROD: "production", STAGING: "staging", DEV: "development" };
    const appEnv = envMap[envFilter] || envFilter.toLowerCase();
    return path.join(outputDir, `${baseName}-${appEnv}.txt`);
  }
  
  // No filter - return legacy path (will be handled specially in readRecentLogs)
  return path.join(outputDir, `${baseName}.txt`);
}

/**
 * Get all environment-specific log files that exist
 */
function getAllEnvLogFiles(outputDir, type) {
  const baseName = type === "error" ? "error-log" : "info-log";
  const envs = ["production", "staging", "development"];
  const files = [];
  
  for (const env of envs) {
    const filePath = path.join(outputDir, `${baseName}-${env}.txt`);
    if (fs.existsSync(filePath)) {
      files.push({ path: filePath, env });
    }
  }
  
  // Also check legacy file
  const legacyPath = path.join(outputDir, `${baseName}.txt`);
  if (fs.existsSync(legacyPath)) {
    files.push({ path: legacyPath, env: "legacy" });
  }
  
  return files;
}

function readRecentLogs({ outputDir, type = "info", lines = 50, envFilter = null }) {
  // If specific environment filter, read from that env's file
  if (envFilter) {
    const logFile = getLogFilePath(outputDir, type, envFilter);
    if (!fs.existsSync(logFile)) {
      return { logs: [], totalLines: 0, filtered: true };
    }

    const content = fs.readFileSync(logFile, "utf8");
    const allLines = content.split("\n").filter((l) => l.trim());
    const recentLines = allLines.slice(-lines);
    return { logs: recentLines, totalLines: allLines.length, filtered: true };
  }
  
  // No filter - read from all environment files and merge
  const envFiles = getAllEnvLogFiles(outputDir, type);
  
  if (envFiles.length === 0) {
    return { logs: [], totalLines: 0, filtered: false };
  }
  
  // Collect all lines with timestamps for sorting
  let allLines = [];
  for (const { path: filePath } of envFiles) {
    const content = fs.readFileSync(filePath, "utf8");
    const fileLines = content.split("\n").filter((l) => l.trim());
    allLines = allLines.concat(fileLines);
  }
  
  // Sort by timestamp (lines start with [YYYY-MM-DD HH:mm:ss])
  allLines.sort((a, b) => {
    const matchA = a.match(/^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]/);
    const matchB = b.match(/^\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]/);
    if (matchA && matchB) {
      return matchA[1].localeCompare(matchB[1]);
    }
    return 0;
  });
  
  const recentLines = allLines.slice(-lines);
  return { logs: recentLines, totalLines: allLines.length, filtered: false };
}

function listArchivedLogs(outputDir) {
  if (!fs.existsSync(outputDir)) return [];

  return fs
    .readdirSync(outputDir)
    .filter((f) => f.includes("-archived-"))
    .map((f) => {
      const full = path.join(outputDir, f);
      const stat = fs.statSync(full);
      return {
        name: f,
        path: `/api/logs/archived/${f}`,
        size: stat.size,
        modified: stat.mtime,
      };
    })
    .sort((a, b) => new Date(b.modified) - new Date(a.modified));
}

function safeArchivedLogPath(outputDir, filename) {
  if (filename.includes("..") || filename.includes("/")) {
    const err = new Error("Invalid filename");
    err.code = "INVALID_FILENAME";
    throw err;
  }

  return path.join(outputDir, filename);
}

function clearLogs({ outputDir, envLabel, envOnly }) {
  const timestamp = new Date().toISOString().replace("T", " ").substring(0, 19);
  
  // Map envLabel to appEnv for file naming
  const envMap = { PROD: "production", STAGING: "staging", DEV: "development" };
  const appEnv = envMap[envLabel] || envLabel.toLowerCase();
  
  // Environment-specific log files
  const envInfoLog = path.join(outputDir, `info-log-${appEnv}.txt`);
  const envErrorLog = path.join(outputDir, `error-log-${appEnv}.txt`);
  
  // Legacy shared log files
  const legacyInfoLog = path.join(outputDir, "info-log.txt");
  const legacyErrorLog = path.join(outputDir, "error-log.txt");
  
  const clearMsg = `[${timestamp}] [${envLabel}] Log file cleared via admin UI\n`;

  if (envOnly) {
    // Clear only the environment-specific log files
    if (fs.existsSync(envInfoLog)) fs.writeFileSync(envInfoLog, clearMsg);
    if (fs.existsSync(envErrorLog)) fs.writeFileSync(envErrorLog, clearMsg);
    
    // Also filter this environment's entries from legacy files (backwards compat)
    [legacyInfoLog, legacyErrorLog].forEach((logFile) => {
      if (!fs.existsSync(logFile)) return;
      const content = fs.readFileSync(logFile, "utf8");
      const lines = content.split("\n");
      const filteredLines = lines.filter((line) => !line.includes(`[${envLabel}]`));
      fs.writeFileSync(logFile, filteredLines.join("\n") + "\n");
    });

    return { message: `Logs for ${envLabel} environment cleared` };
  }

  // Clear all - both env-specific and legacy files
  const allEnvFiles = [
    path.join(outputDir, "info-log-production.txt"),
    path.join(outputDir, "error-log-production.txt"),
    path.join(outputDir, "info-log-staging.txt"),
    path.join(outputDir, "error-log-staging.txt"),
    path.join(outputDir, "info-log-development.txt"),
    path.join(outputDir, "error-log-development.txt"),
    legacyInfoLog,
    legacyErrorLog,
  ];
  
  allEnvFiles.forEach((logFile) => {
    if (fs.existsSync(logFile)) fs.writeFileSync(logFile, clearMsg);
  });

  return { message: "All log files cleared" };
}

module.exports = {
  readRecentLogs,
  listArchivedLogs,
  safeArchivedLogPath,
  clearLogs,
};
