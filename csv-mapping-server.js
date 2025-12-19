/**
 * csv-mapping-server.js
 * Express server for CSV mapping UI with progress tracking and file management
 */
const multer = require("multer");
const express = require("express");
const path = require("path");
const fs = require("fs");


// S3 Client setup (adjust region as needed)
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const s3Client = new S3Client({ region: process.env.AWS_REGION || process.env.AWS_REGION_NAME || "us-west-1" });

const app = express();
const PORT = process.env.CSV_MAPPING_PORT || 4000;

app.use(express.json());
app.use(express.static(path.join(__dirname, "csv-mapping-ui")));

const MAPPINGS_PATH = path.join(__dirname, "csv-mappings.json");

// S3 bucket selection based on execution mode
const executionMode = process.env.EXECUTION_MODE || "production";
const S3_BUCKET_NAME = (executionMode === "development" || executionMode === "test")
  ? process.env.S3_BUCKET_NAME_TEST
  : process.env.S3_BUCKET_NAME;

console.log("[csv-mapping-ui] Using S3 bucket:", S3_BUCKET_NAME, "| Mode:", executionMode);

// Multer for file uploads
const uploadDir = path.join(__dirname, "tmp-uploads");
if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

function readMappings() {
  if (!fs.existsSync(MAPPINGS_PATH)) {
    fs.writeFileSync(MAPPINGS_PATH, JSON.stringify({ files: [] }, null, 2));
  }
  const data = JSON.parse(fs.readFileSync(MAPPINGS_PATH, "utf8"));
  // Handle both array and object formats
  if (Array.isArray(data)) {
    return { files: data };
  }
  return data.files ? data : { files: [] };
}

function writeMappings(data) {
  fs.writeFileSync(MAPPINGS_PATH, JSON.stringify(data, null, 2));
}

function registerNewCsv(fileKey, headers) {
  const mappings = readMappings();
  const existing = mappings.files.find((f) => f.fileKey === fileKey);
  if (existing) {
    existing.headers = headers;
    existing.uploadedAt = new Date().toISOString();
  } else {
    mappings.files.push({
      fileKey,
      status: "pending",
      headers,
      mapping: null,
      uploadedAt: new Date().toISOString(),
    });
  }
  writeMappings(mappings);
}

// ============================================================================
// BASIC ENDPOINTS
// ============================================================================

// GET /api/csv-mappings - List all registered CSV files
app.get("/api/csv-mappings", (req, res) => {
  const mappings = readMappings();
  res.json(mappings.files);
});

// GET /api/csv-mappings - List all registered CSV files
app.get("/api/csv-mappings", (req, res) => {
  try {
    const mappingsFile = path.join(__dirname, "csv-mappings.json");
    if (!fs.existsSync(mappingsFile)) {
      return res.json([]);
    }
    const mappings = JSON.parse(fs.readFileSync(mappingsFile, "utf8"));
    const files = Object.values(mappings);
    res.json(files);
  } catch (err) {
    console.error("[csv-mappings] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/csv-mappings/:fileKey - Get single file details
app.get("/api/csv-mappings/:fileKey", (req, res) => {
  const fileKey = decodeURIComponent(req.params.fileKey);
  const mappings = readMappings();
  const file = mappings.files.find((f) => f.fileKey === fileKey);
  if (!file) return res.status(404).json({ error: "File not found" });
  res.json(file);
});

// POST /api/csv-mappings/:fileKey - Update mapping for a file
app.post("/api/csv-mappings/:fileKey", (req, res) => {
  const fileKey = decodeURIComponent(req.params.fileKey);
  const { partNumber, category, manufacturer } = req.body;
  
  const mappings = readMappings();
  const file = mappings.files.find((f) => f.fileKey === fileKey);
  if (!file) return res.status(404).json({ error: "File not found" });
  
  file.mapping = { partNumber, category, manufacturer };
  file.status = "ready";
  writeMappings(mappings);
  
  res.json({ success: true, file });
});

// DELETE /api/csv-mappings/:fileKey - Delete a file registration
app.delete("/api/csv-mappings/:fileKey", (req, res) => {
  const fileKey = decodeURIComponent(req.params.fileKey);
  const mappings = readMappings();
  const initialLength = mappings.files.length;
  mappings.files = mappings.files.filter((f) => f.fileKey !== fileKey);
  
  if (mappings.files.length === initialLength) {
    return res.status(404).json({ error: "File not found" });
  }
  
  writeMappings(mappings);
  res.json({ success: true, message: `Deleted ${fileKey}` });
});

// POST /api/upload-csv - Upload CSV to S3
app.post("/api/upload-csv", upload.single("file"), async (req, res) => {
  const tempPath = req.file?.path;
  try {
    if (!req.file) {
      return res.status(400).json({ error: "No file uploaded" });
    }
    if (!S3_BUCKET_NAME) {
      return res.status(500).json({ error: "S3_BUCKET_NAME env var is not set." });
    }

    const originalName = req.file.originalname;
    let folder = (req.body.folder || "").trim().replace(/^\/+|\/+$/g, "");
    const s3Key = folder ? `${folder}/${originalName}` : originalName;

    const fileContent = fs.readFileSync(tempPath);
    
    // Parse CSV headers
    const text = fileContent.toString("utf8");
    const lines = text.split(/\r?\n/).filter((l) => l.trim());
    const headers = lines[0] ? lines[0].split(",").map((h) => h.trim()) : [];

    // Upload to S3
    await s3Client.send(
      new PutObjectCommand({
        Bucket: S3_BUCKET_NAME,
        Key: s3Key,
        Body: fileContent,
        ContentType: "text/csv",
      })
    );

    // Register in mappings
    registerNewCsv(s3Key, headers);

    res.json({ success: true, fileKey: s3Key, bucket: S3_BUCKET_NAME, headers });
  } catch (err) {
    console.error("Upload error:", err);
    res.status(500).json({ error: err.message });
  } finally {
    if (tempPath && fs.existsSync(tempPath)) {
      fs.unlinkSync(tempPath);
      console.log("[upload-csv] Cleaned up temp file:", tempPath);
    }
  }
});

// ============================================================================
// PROGRESS & ACTION ENDPOINTS
// ============================================================================

// GET /api/csv-mappings/:fileKey/progress - Get processing progress
app.get("/api/csv-mappings/:fileKey/progress", async (req, res) => {
  try {
    const fileKey = decodeURIComponent(req.params.fileKey);
    const cleanFileKey = fileKey.replace(/\.csv$/i, "").replace(/\//g, "_");
    
    let progress = {
      status: "pending",
      processedRows: 0,
      totalRows: 0,
      updated: 0,
      skipped: 0,
      failed: 0,
      percentage: 0,
      missingProductsCount: 0,
      missingProductsFile: null
    };
    
    // Read from process_checkpoint.json
    const checkpointPath = path.join(__dirname, "process_checkpoint.json");
    if (fs.existsSync(checkpointPath)) {
      try {
        const checkpoint = JSON.parse(fs.readFileSync(checkpointPath, "utf8"));
        if (checkpoint[fileKey]) {
          const cp = checkpoint[fileKey];
          if (cp.rowLevel) {
            progress.processedRows = cp.rowLevel.lastProcessedRow || cp.rowLevel.completedRows || 0;
            progress.totalRows = cp.rowLevel.totalRows || 0;
            progress.updated = cp.rowLevel.updated || 0;
            progress.skipped = cp.rowLevel.skipped || 0;
            progress.failed = cp.rowLevel.failed || 0;
          } else {
            progress.processedRows = cp.lastProcessedRow || 0;
            progress.totalRows = cp.totalRows || 0;
          }
          
          if (progress.totalRows > 0) {
            progress.percentage = Math.round((progress.processedRows / progress.totalRows) * 100);
            progress.status = progress.processedRows >= progress.totalRows ? "completed" : "processing";
          }
        }
      } catch (e) {
        console.error("Error reading checkpoint:", e.message);
      }
    }
    
    // Check batch_status directory as fallback
    const batchStatusDir = path.join(__dirname, "batch_status");
    const fileKeyPath = fileKey.replace(/\.csv$/i, "").replace(/\//g, "/");
    const batchStatusFile = path.join(batchStatusDir, fileKeyPath, "batch_status.json");
    
    if (fs.existsSync(batchStatusFile) && progress.totalRows === 0) {
      try {
        const batchStatus = JSON.parse(fs.readFileSync(batchStatusFile, "utf8"));
        if (batchStatus.totalRows) {
          progress.totalRows = batchStatus.totalRows;
          progress.processedRows = batchStatus.processedRows || batchStatus.completedRows || 0;
          progress.updated = batchStatus.updated || 0;
          progress.skipped = batchStatus.skipped || 0;
          progress.failed = batchStatus.failed || 0;
          progress.percentage = Math.round((progress.processedRows / progress.totalRows) * 100);
          progress.status = progress.processedRows >= progress.totalRows ? "completed" : "processing";
        }
      } catch (e) {
        console.error("Error reading batch status:", e.message);
      }
    }
    
    // Check for missing products
    const missingProductsDir = path.join(__dirname, "missing-products");
    if (fs.existsSync(missingProductsDir)) {
      try {
        const categories = fs.readdirSync(missingProductsDir);
        for (const cat of categories) {
          const catPath = path.join(missingProductsDir, cat);
          if (fs.statSync(catPath).isDirectory()) {
            const files = fs.readdirSync(catPath);
            const matchingFile = files.find(f => f.includes(cleanFileKey));
            if (matchingFile) {
              const filePath = path.join(catPath, matchingFile);
              const missingData = JSON.parse(fs.readFileSync(filePath, "utf8"));
              progress.missingProductsCount = missingData.length;
              progress.missingProductsFile = `${cat}/${matchingFile}`;
              break;
            }
          }
        }
      } catch (e) {
        console.error("Error reading missing products:", e.message);
      }
    }
    
    res.json(progress);
  } catch (err) {
    console.error("Progress endpoint error:", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/csv-mappings/:fileKey/reprocess - Reset and reprocess a file
app.post("/api/csv-mappings/:fileKey/reprocess", async (req, res) => {
  try {
    const fileKey = decodeURIComponent(req.params.fileKey);
    const cleanFileKey = fileKey.replace(/\.csv$/i, "").replace(/\//g, "_");
    
    console.log(`[reprocess] Starting reprocess for: ${fileKey}`);
    
    // 1. Remove from checkpoint
    const checkpointPath = path.join(__dirname, "process_checkpoint.json");
    if (fs.existsSync(checkpointPath)) {
      try {
        const checkpoint = JSON.parse(fs.readFileSync(checkpointPath, "utf8"));
        if (checkpoint[fileKey]) {
          delete checkpoint[fileKey];
          fs.writeFileSync(checkpointPath, JSON.stringify(checkpoint, null, 2));
          console.log(`[reprocess] Removed ${fileKey} from checkpoint`);
        }
      } catch (e) {
        console.error("Error updating checkpoint:", e.message);
      }
    }
    
    // 2. Remove batch_status for this file
    const fileKeyPath = fileKey.replace(/\.csv$/i, "");
    const batchDir = path.join(__dirname, "batch_status", path.dirname(fileKeyPath));
    const specificBatchDir = path.join(__dirname, "batch_status", fileKeyPath);
    
    if (fs.existsSync(specificBatchDir)) {
      fs.rmSync(specificBatchDir, { recursive: true, force: true });
      console.log(`[reprocess] Removed batch_status: ${specificBatchDir}`);
    }
    if (fs.existsSync(batchDir) && batchDir !== path.join(__dirname, "batch_status")) {
      try {
        const remaining = fs.readdirSync(batchDir);
        if (remaining.length === 0) {
          fs.rmSync(batchDir, { recursive: true, force: true });
        }
      } catch (e) {}
    }
    
    // 3. Remove missing products files for this file
    const missingProductsDir = path.join(__dirname, "missing-products");
    if (fs.existsSync(missingProductsDir)) {
      const categories = fs.readdirSync(missingProductsDir);
      for (const cat of categories) {
        const catPath = path.join(missingProductsDir, cat);
        if (fs.statSync(catPath).isDirectory()) {
          const files = fs.readdirSync(catPath);
          for (const f of files) {
            if (f.includes(cleanFileKey)) {
              fs.unlinkSync(path.join(catPath, f));
              console.log(`[reprocess] Removed missing products file: ${cat}/${f}`);
            }
          }
        }
      }
    }
    
    // 4. Clear Redis state for this file
    try {
      const { createClient } = require("redis");
      const redis = createClient({ url: "redis://127.0.0.1:6379/1" });
      await redis.connect();
      
      // Get all keys and delete ones matching this file
      const keys = await redis.keys("*");
      for (const key of keys) {
        if (key.includes(cleanFileKey) || key.includes(fileKey)) {
          await redis.del(key);
          console.log(`[reprocess] Deleted Redis key: ${key}`);
        }
      }
      await redis.quit();
    } catch (e) {
      console.error("[reprocess] Redis cleanup error:", e.message);
    }
    
    // 5. Update file status to "ready"
    const mappings = readMappings();
    const fileIndex = mappings.files.findIndex(f => f.fileKey === fileKey);
    if (fileIndex !== -1) {
      mappings.files[fileIndex].status = "ready";
      writeMappings(mappings);
      console.log(`[reprocess] Set ${fileKey} status to ready`);
    }
    
    res.json({ success: true, message: `File ${fileKey} queued for reprocessing` });
  } catch (err) {
    console.error("Reprocess endpoint error:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/missing-products/:fileKey - Get missing products for a file
app.get("/api/missing-products/:fileKey", async (req, res) => {
  try {
    const fileKey = decodeURIComponent(req.params.fileKey);
    const cleanFileKey = fileKey.replace(/\.csv$/i, "").replace(/\//g, "_");
    
    const missingProductsDir = path.join(__dirname, "missing-products");
    let allMissing = [];
    let categories = [];
    
    if (fs.existsSync(missingProductsDir)) {
      const catDirs = fs.readdirSync(missingProductsDir);
      for (const cat of catDirs) {
        const catPath = path.join(missingProductsDir, cat);
        if (fs.statSync(catPath).isDirectory()) {
          const files = fs.readdirSync(catPath);
          const matchingFile = files.find(f => f.includes(cleanFileKey));
          if (matchingFile) {
            const filePath = path.join(catPath, matchingFile);
            try {
              const missingData = JSON.parse(fs.readFileSync(filePath, "utf8"));
              allMissing = allMissing.concat(missingData);
              categories.push(cat.replace("missing-", ""));
            } catch (e) {
              console.error("Error reading missing products:", e.message);
            }
          }
        }
      }
    }
    
    res.json({ 
      count: allMissing.length, 
      categories: [...new Set(categories)],
      products: allMissing 
    });
  } catch (err) {
    console.error("Missing products endpoint error:", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/missing-products/:fileKey/create - Trigger create-missing-products script
app.post("/api/missing-products/:fileKey/create", async (req, res) => {
  try {
    const fileKey = decodeURIComponent(req.params.fileKey);
    const { categorySlug } = req.body;
    
    if (!categorySlug) {
      return res.status(400).json({ error: "categorySlug is required" });
    }
    
    const { exec } = require("child_process");
    const command = `node create-missing-products.js ${categorySlug} "${fileKey}"`;
    
    console.log(`[create-products] Running: ${command}`);
    
    exec(command, { cwd: __dirname }, (error, stdout, stderr) => {
      if (error) {
        console.error("Create products error:", error);
        return res.status(500).json({ error: error.message, stderr });
      }
      console.log("[create-products] Output:", stdout);
      res.json({ success: true, output: stdout });
    });
  } catch (err) {
    console.error("Create products endpoint error:", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/trigger-processing - Restart woo-update-app
app.post("/api/trigger-processing", async (req, res) => {
  try {
    const { exec } = require("child_process");
    
    console.log("[trigger-processing] Restarting woo-update-app...");
    
    exec("pm2 restart woo-update-app", (error, stdout, stderr) => {
      if (error) {
        console.error("Trigger processing error:", error);
        return res.status(500).json({ error: error.message });
      }
      console.log("[trigger-processing] Success:", stdout);
      res.json({ success: true, message: "Processing triggered" });
    });
  } catch (err) {
    console.error("Trigger processing error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================================
// START SERVER
// ============================================================================

app.listen(PORT, () => {
  console.log(`CSV Mapping UI running on http://localhost:${PORT}`);
});