const fs = require("fs");
const path = require("path");

function getLogFilePath(outputDir, type) {
  return type === "error"
    ? path.join(outputDir, "error-log.txt")
    : path.join(outputDir, "info-log.txt");
}

function readRecentLogs({ outputDir, type = "info", lines = 50, envFilter = null }) {
  const logFile = getLogFilePath(outputDir, type);
  if (!fs.existsSync(logFile)) {
    return { logs: [], totalLines: 0, filtered: Boolean(envFilter) };
  }

  const content = fs.readFileSync(logFile, "utf8");
  let allLines = content.split("\n").filter((l) => l.trim());

  if (envFilter) {
    allLines = allLines.filter((line) => line.includes(`[${envFilter}]`));
  }

  const recentLines = allLines.slice(-lines);
  return { logs: recentLines, totalLines: allLines.length, filtered: Boolean(envFilter) };
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
  const infoLog = path.join(outputDir, "info-log.txt");
  const errorLog = path.join(outputDir, "error-log.txt");

  const timestamp = new Date().toISOString().replace("T", " ").substring(0, 19);

  if (envOnly) {
    [infoLog, errorLog].forEach((logFile) => {
      if (!fs.existsSync(logFile)) return;
      const content = fs.readFileSync(logFile, "utf8");
      const lines = content.split("\n");
      const filteredLines = lines.filter((line) => !line.includes(`[${envLabel}]`));
      const newContent =
        filteredLines.join("\n") +
        `\n[${timestamp}] [${envLabel}] Logs for ${envLabel} cleared via admin UI\n`;
      fs.writeFileSync(logFile, newContent);
    });

    return { message: `Logs for ${envLabel} environment cleared` };
  }

  const clearMsg = `[${timestamp}] [${envLabel}] Log file cleared via admin UI\n`;
  if (fs.existsSync(infoLog)) fs.writeFileSync(infoLog, clearMsg);
  if (fs.existsSync(errorLog)) fs.writeFileSync(errorLog, clearMsg);

  return { message: "All log files cleared" };
}

module.exports = {
  readRecentLogs,
  listArchivedLogs,
  safeArchivedLogPath,
  clearLogs,
};
