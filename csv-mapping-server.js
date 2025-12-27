/**
 * csv-mapping-server.js
 * Express server for CSV mapping UI with progress tracking and file management
 * 
 * ENHANCEMENTS:
 * - Environment info endpoint (/api/environment)
 * - Environment-aware log filtering
 * - Clear logs by environment option
 * - Archived logs listing
 */

const multer = require("multer");
const express = require("express");
const path = require("path");
const fs = require("fs");
const { exec } = require("child_process");

// S3 Client setup
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const s3Client = new S3Client({ 
  region: process.env.AWS_REGION || process.env.AWS_REGION_NAME || "us-west-1" 
});

const app = express();
const PORT = process.env.CSV_MAPPING_PORT || 4000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "csv-mapping-ui")));

const MAPPINGS_PATH = path.join(__dirname, "csv-mappings.json");

// =============================================================================
// ENVIRONMENT CONFIGURATION
// =============================================================================

const executionMode = process.env.EXECUTION_MODE || "production";

/**
 * Get environment label for display
 */
const getEnvLabel = () => {
  switch (executionMode) {
    case "production": return "PROD";
    case "test": return "STAGING";
    case "development": return "DEV";
    default: return executionMode.toUpperCase();
  }
};

const ENV_LABEL = getEnvLabel();

const S3_BUCKET_NAME = (executionMode === "development" || executionMode === "test")
  ? process.env.S3_BUCKET_NAME_TEST
  : process.env.S3_BUCKET_NAME;

console.log(`[csv-mapping-ui] Environment: ${ENV_LABEL} | S3 Bucket: ${S3_BUCKET_NAME}`);

// Multer for file uploads
const uploadDir = path.join(__dirname, "tmp-uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });

// =============================================================================
// HELPER FUNCTIONS
// =============================================================================

function readMappings() {
  if (!fs.existsSync(MAPPINGS_PATH)) {
    fs.writeFileSync(MAPPINGS_PATH, JSON.stringify({ files: [] }, null, 2));
  }
  const data = JSON.parse(fs.readFileSync(MAPPINGS_PATH, "utf8"));
  if (Array.isArray(data)) {
    return { files: data };
  }
  return data.files ? data : { files: [] };
}

function writeMappings(data) {
  fs.writeFileSync(MAPPINGS_PATH, JSON.stringify(data, null, 2));
}

// =============================================================================
// ENVIRONMENT ENDPOINT
// =============================================================================

/**
 * GET /api/environment
 * Returns current environment information for the UI
 */
app.get("/api/environment", (req, res) => {
  res.json({
    mode: executionMode,
    label: ENV_LABEL,
    bucket: S3_BUCKET_NAME,
    timestamp: new Date().toISOString(),
  });
});

// =============================================================================
// LOGS ENDPOINT (Enhanced with environment filtering)
// =============================================================================

/**
 * GET /api/logs
 * Get recent log entries with optional environment filtering
 * 
 * Query params:
 * - lines: Number of lines to return (default: 50)
 * - type: "info" or "error" (default: "info")
 * - env: Filter by environment label (optional, e.g., "PROD", "STAGING", "DEV")
 */
app.get("/api/logs", (req, res) => {
  try {
    const lines = parseInt(req.query.lines) || 50;
    const type = req.query.type || "info";
    const envFilter = req.query.env || null;
    
    const logFile = type === "error" 
      ? path.join(__dirname, "output-files", "error-log.txt")
      : path.join(__dirname, "output-files", "info-log.txt");
    
    if (!fs.existsSync(logFile)) {
      return res.json({ logs: [], environment: ENV_LABEL });
    }
    
    const content = fs.readFileSync(logFile, "utf8");
    let allLines = content.split("\n").filter(l => l.trim());
    
    // Filter by environment if specified
    if (envFilter) {
      allLines = allLines.filter(line => line.includes(`[${envFilter}]`));
    }
    
    const recentLines = allLines.slice(-lines);
    
    res.json({ 
      logs: recentLines,
      environment: ENV_LABEL,
      totalLines: allLines.length,
      filtered: !!envFilter,
    });
  } catch (err) {
    console.error("[logs] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/logs/archived
 * List archived log files
 */
app.get("/api/logs/archived", (req, res) => {
  try {
    const outputDir = path.join(__dirname, "output-files");
    
    if (!fs.existsSync(outputDir)) {
      return res.json({ archives: [] });
    }
    
    const files = fs.readdirSync(outputDir)
      .filter(f => f.includes("-archived-"))
      .map(f => ({
        name: f,
        path: `/api/logs/archived/${f}`,
        size: fs.statSync(path.join(outputDir, f)).size,
        modified: fs.statSync(path.join(outputDir, f)).mtime,
      }))
      .sort((a, b) => new Date(b.modified) - new Date(a.modified));
    
    res.json({ archives: files });
  } catch (err) {
    console.error("[logs/archived] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

/**
 * GET /api/logs/archived/:filename
 * Download a specific archived log file
 */
app.get("/api/logs/archived/:filename", (req, res) => {
  try {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, "output-files", filename);
    
    // Security: Ensure filename doesn't contain path traversal
    if (filename.includes("..") || filename.includes("/")) {
      return res.status(400).json({ error: "Invalid filename" });
    }
    
    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: "File not found" });
    }
    
    res.download(filePath);
  } catch (err) {
    console.error("[logs/archived/:filename] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// SYSTEM STATUS ENDPOINT
// =============================================================================

/**
 * GET /api/system-status
 * Get overall system status including environment info
 */
app.get("/api/system-status", async (req, res) => {
  try {
    const { createClient } = require("redis");
    const redis = createClient({ url: "redis://127.0.0.1:6379/1" });
    await redis.connect();
    
    const keys = await redis.keys("*");
    await redis.quit();
    
    const checkpointExists = fs.existsSync(path.join(__dirname, "process_checkpoint.json"));
    const mappings = readMappings();
    
    const readyFiles = mappings.files.filter(f => f.status === "ready").length;
    const processingFiles = mappings.files.filter(f => f.status === "processing").length;
    const completedFiles = mappings.files.filter(f => f.status === "completed").length;
    
    // Count archived log files
    const outputDir = path.join(__dirname, "output-files");
    let archivedCount = 0;
    if (fs.existsSync(outputDir)) {
      archivedCount = fs.readdirSync(outputDir).filter(f => f.includes("-archived-")).length;
    }
    
    res.json({
      environment: {
        mode: executionMode,
        label: ENV_LABEL,
        bucket: S3_BUCKET_NAME,
      },
      redis: {
        connected: true,
        keyCount: keys.length,
      },
      checkpoint: checkpointExists,
      files: {
        ready: readyFiles,
        processing: processingFiles,
        completed: completedFiles,
        total: mappings.files.length,
      },
      logs: {
        archivedCount: archivedCount,
      },
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error("[system-status] Error:", err);
    res.status(500).json({ 
      error: err.message,
      environment: { mode: executionMode, label: ENV_LABEL },
    });
  }
});

// =============================================================================
// ADMIN ENDPOINTS
// =============================================================================

/**
 * POST /api/admin/restart-workers
 * Restart PM2 workers
 */
app.post("/api/admin/restart-workers", (req, res) => {
  console.log(`[admin] [${ENV_LABEL}] Restarting workers...`);
  
  exec("pm2 restart woo-update-app woo-worker", (error, stdout, stderr) => {
    if (error) {
      console.error(`[admin] Restart error: ${error.message}`);
      return res.status(500).json({ error: error.message });
    }
    
    console.log(`[admin] Restart output: ${stdout}`);
    res.json({ 
      success: true, 
      message: "Workers restarted",
      environment: ENV_LABEL,
      timestamp: new Date().toISOString(),
    });
  });
});

/**
 * POST /api/admin/flush-redis
 * Flush Redis database
 */
app.post("/api/admin/flush-redis", async (req, res) => {
  try {
    console.log(`[admin] [${ENV_LABEL}] Flushing Redis...`);
    
    const { createClient } = require("redis");
    const redis = createClient({ url: "redis://127.0.0.1:6379/1" });
    await redis.connect();
    await redis.flushDb();
    await redis.quit();
    
    console.log(`[admin] [${ENV_LABEL}] Redis flushed`);
    
    res.json({ 
      success: true, 
      message: "Redis flushed",
      environment: ENV_LABEL,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error(`[admin] Flush error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/admin/full-reset
 * Full system reset: stop workers, flush Redis, clear checkpoints
 */
app.post("/api/admin/full-reset", async (req, res) => {
  try {
    console.log(`[admin] [${ENV_LABEL}] Full reset initiated...`);
    
    // Step 1: Stop workers
    await new Promise((resolve) => {
      exec("pm2 stop woo-update-app woo-worker 2>/dev/null", () => resolve());
    });
    
    // Step 2: Flush Redis
    const { createClient } = require("redis");
    const redis = createClient({ url: "redis://127.0.0.1:6379/1" });
    await redis.connect();
    await redis.flushDb();
    await redis.quit();
    
    // Step 3: Clear checkpoint file
    const checkpointPath = path.join(__dirname, "process_checkpoint.json");
    if (fs.existsSync(checkpointPath)) {
      fs.unlinkSync(checkpointPath);
    }
    
    // Step 4: Reset file statuses in mappings
    const mappings = readMappings();
    mappings.files.forEach(file => {
      if (file.status === "processing") {
        file.status = "ready";
      }
    });
    writeMappings(mappings);
    
    // Step 5: Start workers
    exec("pm2 start woo-update-app woo-worker 2>/dev/null", () => {});
    
    console.log(`[admin] [${ENV_LABEL}] Full reset completed`);
    
    res.json({ 
      success: true, 
      message: "Full reset completed. Workers restarting.",
      environment: ENV_LABEL,
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error(`[admin] Full reset error: ${err.message}`);
    exec("pm2 start woo-update-app woo-worker 2>/dev/null", () => {});
    res.status(500).json({ error: err.message });
  }
});

/**
 * POST /api/admin/clear-logs
 * Clear log files with optional environment filtering
 * 
 * Body params:
 * - envOnly: If true, only clear logs for current environment (default: false)
 */
app.post("/api/admin/clear-logs", (req, res) => {
  try {
    const envOnly = req.body?.envOnly || false;
    
    console.log(`[admin] [${ENV_LABEL}] Clearing log files (envOnly: ${envOnly})...`);
    
    const infoLog = path.join(__dirname, "output-files", "info-log.txt");
    const errorLog = path.join(__dirname, "output-files", "error-log.txt");
    
    const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
    
    if (envOnly) {
      // Filter out only lines for current environment
      [infoLog, errorLog].forEach(logFile => {
        if (fs.existsSync(logFile)) {
          const content = fs.readFileSync(logFile, "utf8");
          const lines = content.split("\n");
          const filteredLines = lines.filter(line => !line.includes(`[${ENV_LABEL}]`));
          const newContent = filteredLines.join("\n") + 
            `\n[${timestamp}] [${ENV_LABEL}] Logs for ${ENV_LABEL} cleared via admin UI\n`;
          fs.writeFileSync(logFile, newContent);
        }
      });
      
      res.json({ 
        success: true, 
        message: `Logs for ${ENV_LABEL} environment cleared`,
        environment: ENV_LABEL,
        timestamp: new Date().toISOString(),
      });
    } else {
      // Clear all logs
      const clearMsg = `[${timestamp}] [${ENV_LABEL}] Log file cleared via admin UI\n`;
      
      if (fs.existsSync(infoLog)) {
        fs.writeFileSync(infoLog, clearMsg);
      }
      if (fs.existsSync(errorLog)) {
        fs.writeFileSync(errorLog, clearMsg);
      }
      
      res.json({ 
        success: true, 
        message: "All log files cleared",
        environment: ENV_LABEL,
        timestamp: new Date().toISOString(),
      });
    }
    
    console.log(`[admin] [${ENV_LABEL}] Log files cleared`);
  } catch (err) {
    console.error(`[admin] Clear logs error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// CSV MAPPINGS ENDPOINTS
// =============================================================================

app.get("/api/csv-mappings", (req, res) => {
  try {
    const mappings = readMappings();
    res.json(mappings);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post("/api/csv-mappings", (req, res) => {
  try {
    const { fileKey, mapping, status } = req.body;
    const mappings = readMappings();
    
    const existingIndex = mappings.files.findIndex(f => f.fileKey === fileKey);
    
    if (existingIndex >= 0) {
      mappings.files[existingIndex] = { 
        ...mappings.files[existingIndex], 
        mapping, 
        status,
        updatedAt: new Date().toISOString(),
      };
    } else {
      mappings.files.push({ 
        fileKey, 
        mapping, 
        status,
        createdAt: new Date().toISOString(),
      });
    }
    
    writeMappings(mappings);
    res.json({ success: true, file: mappings.files.find(f => f.fileKey === fileKey) });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete("/api/csv-mappings/:fileKey", (req, res) => {
  try {
    const fileKey = decodeURIComponent(req.params.fileKey);
    const mappings = readMappings();
    
    mappings.files = mappings.files.filter(f => f.fileKey !== fileKey);
    writeMappings(mappings);
    
    res.json({ success: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// FILE UPLOAD ENDPOINT
// =============================================================================

app.post("/api/upload", upload.single("file"), async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }
    
    const folder = req.body.folder || "";
    const fileKey = folder ? `${folder}/${req.file.originalname}` : req.file.originalname;
    
    // Read file and upload to S3
    const fileContent = fs.readFileSync(req.file.path);
    
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET_NAME,
      Key: fileKey,
      Body: fileContent,
      ContentType: "text/csv",
    }));
    
    // Clean up temp file
    fs.unlinkSync(req.file.path);
    
    // Add to mappings
    const mappings = readMappings();
    if (!mappings.files.find(f => f.fileKey === fileKey)) {
      mappings.files.push({
        fileKey,
        status: "pending",
        mapping: null,
        createdAt: new Date().toISOString(),
      });
      writeMappings(mappings);
    }
    
    console.log(`[upload] [${ENV_LABEL}] Uploaded: ${fileKey} to ${S3_BUCKET_NAME}`);
    
    res.json({ 
      success: true, 
      fileKey,
      bucket: S3_BUCKET_NAME,
      environment: ENV_LABEL,
    });
  } catch (err) {
    console.error(`[upload] Error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// TRIGGER PROCESSING ENDPOINT
// =============================================================================

app.post("/api/trigger-processing", (req, res) => {
  console.log(`[trigger] [${ENV_LABEL}] Triggering processing...`);
  
  exec("pm2 restart woo-update-app", (error, stdout, stderr) => {
    if (error) {
      console.error(`[trigger] Error: ${error.message}`);
      return res.status(500).json({ error: error.message });
    }
    
    res.json({ 
      success: true, 
      message: "Processing triggered",
      environment: ENV_LABEL,
    });
  });
});

// =============================================================================
// PROGRESS ENDPOINT
// =============================================================================

app.get("/api/progress", async (req, res) => {
  try {
    const { createClient } = require("redis");
    const redis = createClient({ url: "redis://127.0.0.1:6379/1" });
    await redis.connect();
    
    const fileKeys = await redis.keys("total-rows:*");
    const progress = {};
    
    for (const key of fileKeys) {
      const fileKey = key.replace(/^total-rows:/, "");
      
      const totalRows = parseInt(await redis.get(`total-rows:${fileKey}`) || 0, 10);
      const updated = parseInt(await redis.get(`updated-products:${fileKey}`) || 0, 10);
      const skipped = parseInt(await redis.get(`skipped-products:${fileKey}`) || 0, 10);
      const failed = parseInt(await redis.get(`failed-products:${fileKey}`) || 0, 10);
      
      progress[fileKey] = {
        totalRows,
        updated,
        skipped,
        failed,
        completed: updated + skipped + failed,
        percentage: totalRows > 0 ? Math.round(((updated + skipped + failed) / totalRows) * 100) : 0,
      };
    }
    
    await redis.quit();
    
    res.json({ 
      progress,
      environment: ENV_LABEL,
    });
  } catch (err) {
    console.error(`[progress] Error: ${err.message}`);
    res.status(500).json({ error: err.message });
  }
});

// =============================================================================
// START SERVER
// =============================================================================

app.listen(PORT, () => {
  console.log(`[${ENV_LABEL}] CSV Mapping UI running on http://localhost:${PORT}`);
});