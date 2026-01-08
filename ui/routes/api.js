const express = require("express");
const multer = require("multer");
const fs = require("fs");
const path = require("path");

const { withRedis } = require("../services/redis");
const { progressKeys } = require("../../src/services/queue");
const { readMappings, writeMappings } = require("../services/mappings-store");
const {
  readRecentLogs,
  listArchivedLogs,
  safeArchivedLogPath,
  clearLogs,
} = require("../services/log-files");
const { createS3Client, uploadCsvToS3, getCsvHeadersFromS3 } = require("../services/s3");
const { restartApp, restartAppQuantityOnly, restartAppFullMode } = require("../services/pm2");
const { getQueueActivityCounts, isQueueRunning } = require("../services/bullmq-inspector");
const { 
  listMissingProductFolders, 
  processMissingProducts 
} = require("../../src/missing-products/create-missing-products");

const {
  validateCanStartFromMappings,
  isRunActiveFromProgress,
} = require("./run-status-utils");

function ensureDir(dirPath) {
  if (!fs.existsSync(dirPath)) fs.mkdirSync(dirPath, { recursive: true });
}

function isMappingComplete(mapping) {
  return Boolean(
    mapping &&
      typeof mapping === "object" &&
      typeof mapping.partNumber === "string" &&
      mapping.partNumber.trim() &&
      typeof mapping.manufacturer === "string" &&
      mapping.manufacturer.trim() &&
      typeof mapping.category === "string" &&
      mapping.category.trim()
  );
}

function createApiRouter(config) {
  const router = express.Router();

  // ---- Clear logs (POST)
  // Supports env scoping via ?env=PROD|STAGING|DEV or body.env.
  // If env is omitted, clears all logs.
  router.post("/logs/clear", express.json(), (req, res) => {
    try {
      const envLabel = (req.query.env || req.body?.env || config.envLabel || "ALL").toString();
      const envOnly = Boolean(envLabel) && envLabel !== "ALL";

      const result = clearLogs({
        outputDir: config.paths.outputDir,
        envLabel: envOnly ? envLabel : config.envLabel,
        envOnly,
      });

      res.json(result);
    } catch (err) {
      console.error("[logs/clear] Error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Environment
  router.get("/environment", (req, res) => {
    res.json({
      mode: config.executionMode,
      label: config.envLabel,
      bucket: config.s3BucketName,
      timestamp: new Date().toISOString(),
    });
  });

  // ---- Logs
  router.get("/logs", (req, res) => {
    try {
      const lines = parseInt(req.query.lines, 10) || 50;
      const type = req.query.type || "info";
      const envFilter = req.query.env || null;

      const result = readRecentLogs({
        outputDir: config.paths.outputDir,
        type,
        lines,
        envFilter,
      });

      res.json({
        logs: result.logs,
        environment: config.envLabel,
        totalLines: result.totalLines,
        filtered: result.filtered,
      });
    } catch (err) {
      console.error("[logs] Error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  router.get("/logs/archived", (req, res) => {
    try {
      const files = listArchivedLogs(config.paths.outputDir);
      res.json({ archives: files });
    } catch (err) {
      console.error("[logs/archived] Error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  router.get("/logs/archived/:filename", (req, res) => {
    try {
      const filename = req.params.filename;
      const filePath = safeArchivedLogPath(config.paths.outputDir, filename);

      if (!fs.existsSync(filePath)) {
        return res.status(404).json({ error: "File not found" });
      }

      res.download(filePath);
    } catch (err) {
      if (err.code === "INVALID_FILENAME") {
        return res.status(400).json({ error: "Invalid filename" });
      }
      console.error("[logs/archived/:filename] Error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- System status
  router.get("/system-status", async (req, res) => {
    try {
      const keys = await withRedis(config.redisUrl, async (redis) => {
        return redis.keys("*");
      });

      const checkpointExists = config.paths.checkpointPaths.some((p) => fs.existsSync(p));
      const mappings = readMappings(config.paths.mappingsPath);

      const readyFiles = mappings.files.filter((f) => f.status === "ready").length;
      const processingFiles = mappings.files.filter((f) => f.status === "processing").length;
      const completedFiles = mappings.files.filter((f) => f.status === "completed").length;

      let archivedCount = 0;
      if (fs.existsSync(config.paths.outputDir)) {
        archivedCount = fs
          .readdirSync(config.paths.outputDir)
          .filter((f) => f.includes("-archived-"))
          .length;
      }

      res.json({
        environment: {
          mode: config.executionMode,
          label: config.envLabel,
          bucket: config.s3BucketName,
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
          archivedCount,
        },
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error("[system-status] Error:", err);
      res.status(500).json({
        error: err.message,
        environment: { mode: config.executionMode, label: config.envLabel },
      });
    }
  });

  // ---- CSV mappings
  router.get("/csv-mappings", (req, res) => {
    try {
      const mappings = readMappings(config.paths.mappingsPath);

      // Normalize: do not allow READY when mapping is incomplete.
      // This fixes any historical bad states and prevents processing without required columns.
      let changed = false;
      const now = new Date().toISOString();
      // Also: remove invalid entries (e.g. {} objects) which can confuse the UI.
      const beforeCount = Array.isArray(mappings.files) ? mappings.files.length : 0;
      mappings.files = (mappings.files || []).filter((f) => f && typeof f.fileKey === "string" && f.fileKey.trim());
      if (mappings.files.length !== beforeCount) changed = true;

      for (const f of mappings.files || []) {
        if (!f || !f.fileKey) continue;

        // Keep terminal states.
        if (f.status === "processing" || f.status === "completed") continue;

        // Enforce: mapping incomplete => pending (regardless of what status says).
        // This ensures the UI never shows READY for a file that cannot run.
        if (!isMappingComplete(f.mapping)) {
          if (f.status !== "pending") {
            f.status = "pending";
            f.updatedAt = now;
            changed = true;
          }
        } else {
          // Mapping complete: keep existing status, but normalize unknown/missing to pending.
          if (!f.status || typeof f.status !== "string") {
            f.status = "pending";
            f.updatedAt = now;
            changed = true;
          }
        }
      }
      if (changed) writeMappings(config.paths.mappingsPath, mappings);

      res.json(mappings);
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // Fetch + store CSV headers for a given fileKey (from S3)
  router.get("/csv-mappings/:fileKey/headers", async (req, res) => {
    try {
      const fileKey = decodeURIComponent(req.params.fileKey);

      if (!config.s3BucketName) {
        return res.status(500).json({
          error:
            "S3 bucket is not configured. Set S3_BUCKET_NAME_PRODUCTION/STAGING/DEVELOPMENT (or legacy S3_BUCKET_NAME/S3_BUCKET_NAME_TEST).",
        });
      }

      const headers = await getCsvHeadersFromS3({
        s3Client,
        bucket: config.s3BucketName,
        key: fileKey,
      });

      const mappings = readMappings(config.paths.mappingsPath);
      const idx = mappings.files.findIndex((f) => f.fileKey === fileKey);
      const now = new Date().toISOString();

      if (idx >= 0) {
        mappings.files[idx] = {
          ...mappings.files[idx],
          headers,
          updatedAt: now,
        };
      } else {
        mappings.files.push({
          fileKey,
          status: "pending",
          mapping: null,
          headers,
          createdAt: now,
        });
      }

      writeMappings(config.paths.mappingsPath, mappings);
      res.json({ success: true, fileKey, headers });
    } catch (err) {
      console.error("[csv-mappings/:fileKey/headers] Error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  router.post("/csv-mappings", (req, res) => {
    try {
      const { fileKey, mapping, status, headers } = req.body;
      const mappings = readMappings(config.paths.mappingsPath);

      const existingIndex = mappings.files.findIndex((f) => f.fileKey === fileKey);

      if (existingIndex >= 0) {
        const existing = mappings.files[existingIndex];

        const nextMapping = mapping === undefined ? existing.mapping : mapping;
        let nextStatus = status === undefined ? existing.status : status;
        const nextHeaders = headers === undefined ? existing.headers : headers;

        // Enforce: READY only when mapping is complete.
        if (nextStatus === "ready" && !isMappingComplete(nextMapping)) {
          nextStatus = "pending";
        }

        mappings.files[existingIndex] = {
          ...existing,
          // Only update fields that were provided.
          mapping: nextMapping,
          status: nextStatus,
          headers: nextHeaders,
          updatedAt: new Date().toISOString(),
        };
      } else {
        const nextMapping = mapping === undefined ? null : mapping;
        let nextStatus = status === undefined ? "pending" : status;
        const nextHeaders = headers === undefined ? undefined : headers;
        if (nextStatus === "ready" && !isMappingComplete(nextMapping)) {
          nextStatus = "pending";
        }
        mappings.files.push({
          fileKey,
          mapping: nextMapping,
          status: nextStatus,
          headers: nextHeaders,
          createdAt: new Date().toISOString(),
        });
      }

      writeMappings(config.paths.mappingsPath, mappings);
      res.json({
        success: true,
        file: mappings.files.find((f) => f.fileKey === fileKey),
      });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  router.delete("/csv-mappings/:fileKey", async (req, res) => {
    try {
      const fileKey = decodeURIComponent(req.params.fileKey);
      const mappings = readMappings(config.paths.mappingsPath);

      mappings.files = mappings.files.filter((f) => f.fileKey !== fileKey);
      writeMappings(config.paths.mappingsPath, mappings);

      // Best-effort cleanup: remove progress keys + checkpoint entries so UI stays consistent.
      // Do not fail the request if Redis/checkpoints are unavailable.
      try {
        await withRedis(config.redisUrl, async (redis) => {
          const keys = progressKeys.allKeys(fileKey);
          await redis.del([
            keys.totalRows,
            keys.updated,
            keys.skipped,
            keys.failed,
            keys.processing,
          ]);
        });
      } catch (e) {
        console.error(
          `[csv-mappings:delete] Failed to clear Redis progress for ${fileKey}: ${e.message}`
        );
      }

      for (const checkpointPath of config.paths.checkpointPaths || []) {
        try {
          if (!checkpointPath || !fs.existsSync(checkpointPath)) continue;
          const raw = fs.readFileSync(checkpointPath, "utf-8") || "{}";
          const json = JSON.parse(raw);
          if (json && Object.prototype.hasOwnProperty.call(json, fileKey)) {
            delete json[fileKey];
            fs.writeFileSync(checkpointPath, JSON.stringify(json, null, 2));
          }
        } catch (e) {
          console.error(
            `[csv-mappings:delete] Failed to clear checkpoint for ${fileKey}: ${e.message}`
          );
        }
      }

      res.json({ success: true, fileKey });
    } catch (err) {
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Upload
  ensureDir(config.paths.uploadDir);
  const upload = multer({ dest: config.paths.uploadDir });

  const s3Client = createS3Client(config.s3Region);

  router.post("/upload", upload.single("file"), async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: "No file uploaded" });
      }

      if (!config.s3BucketName) {
        return res.status(500).json({
          error:
            "S3 bucket is not configured. Set S3_BUCKET_NAME_PRODUCTION/STAGING/DEVELOPMENT (or legacy S3_BUCKET_NAME/S3_BUCKET_NAME_TEST).",
        });
      }

      const folder = req.body.folder || "";
      const fileKey = folder ? `${folder}/${req.file.originalname}` : req.file.originalname;

      const fileContent = fs.readFileSync(req.file.path);

      await uploadCsvToS3({
        s3Client,
        bucket: config.s3BucketName,
        key: fileKey,
        body: fileContent,
      });

      fs.unlinkSync(req.file.path);

      const mappings = readMappings(config.paths.mappingsPath);
      if (!mappings.files.find((f) => f.fileKey === fileKey)) {
        mappings.files.push({
          fileKey,
          status: "pending",
          mapping: null,
          createdAt: new Date().toISOString(),
        });
        writeMappings(config.paths.mappingsPath, mappings);
      }

      console.log(
        `[upload] [${config.envLabel}] Uploaded: ${fileKey} to ${config.s3BucketName}`
      );

      res.json({
        success: true,
        fileKey,
        bucket: config.s3BucketName,
        environment: config.envLabel,
      });
    } catch (err) {
      console.error(`[upload] Error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Trigger processing
  router.post("/trigger-processing", async (req, res) => {
    try {
      const mappings = readMappings(config.paths.mappingsPath);

      // Determine run activity primarily via BullMQ queue activity.
      // This avoids stale progress keys from blocking Start forever.
      let alreadyRunning = false;
      try {
        const counts = await getQueueActivityCounts();
        alreadyRunning = isQueueRunning(counts);
      } catch (err) {
        // Fallback: use Redis progress keys (environment-specific).
        const progress = await withRedis(config.redisUrl, async (redis) => {
          const fileKeys = await redis.keys(progressKeys.totalRowsPattern());
          const out = {};

          for (const key of fileKeys) {
            const fk = progressKeys.extractFileKey(key);
            const totalRows = parseInt((await redis.get(progressKeys.totalRows(fk))) || 0, 10);
            const updated = parseInt((await redis.get(progressKeys.updatedProducts(fk))) || 0, 10);
            const skipped = parseInt((await redis.get(progressKeys.skippedProducts(fk))) || 0, 10);
            const failed = parseInt((await redis.get(progressKeys.failedProducts(fk))) || 0, 10);
            const completed = updated + skipped + failed;
            out[fk] = { totalRows, updated, skipped, failed, completed };
          }

          return out;
        });
        alreadyRunning = isRunActiveFromProgress(progress);
      }

      if (alreadyRunning) {
        return res.status(409).json({
          success: false,
          error: "Processing is already running",
          environment: config.envLabel,
        });
      }

      const validation = validateCanStartFromMappings(mappings);
      if (!validation.ok) {
        return res.status(400).json({
          success: false,
          error: "Cannot start processing: missing required mappings",
          reasons: validation.reasons,
          environment: config.envLabel,
        });
      }

      // If user mapped a file but left it as pending, promote it to READY so the worker pipeline will pick it up.
      // (The ingest pipeline processes only READY files.)
      let promoted = 0;
      const now = new Date().toISOString();
      for (const f of mappings.files || []) {
        if (f && f.status === "pending" && f.mapping && typeof f.mapping === "object") {
          const mapped =
            typeof f.mapping.partNumber === "string" && f.mapping.partNumber.trim() &&
            typeof f.mapping.manufacturer === "string" && f.mapping.manufacturer.trim() &&
            typeof f.mapping.category === "string" && f.mapping.category.trim();
          if (mapped) {
            f.status = "ready";
            f.updatedAt = now;
            promoted += 1;
          }
        }
      }

      if (promoted > 0) {
        writeMappings(config.paths.mappingsPath, mappings);
      }

      console.log(`[trigger] [${config.envLabel}] Triggering processing...`);
      await restartApp();
      res.json({
        success: true,
        message: promoted > 0 ? `Processing triggered (auto-marked ${promoted} file(s) READY)` : "Processing triggered",
        environment: config.envLabel,
      });
    } catch (err) {
      console.error(`[trigger] Error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Trigger quantity-only processing (UPDATE_MODE=quantity)
  router.post("/trigger-processing/quantity-only", async (req, res) => {
    try {
      const mappings = readMappings(config.paths.mappingsPath);

      // Check if already running
      let alreadyRunning = false;
      try {
        const counts = await getQueueActivityCounts();
        alreadyRunning = isQueueRunning(counts);
      } catch (err) {
        const progress = await withRedis(config.redisUrl, async (redis) => {
          const fileKeys = await redis.keys(progressKeys.totalRowsPattern());
          const out = {};
          for (const key of fileKeys) {
            const fk = progressKeys.extractFileKey(key);
            const totalRows = parseInt((await redis.get(progressKeys.totalRows(fk))) || 0, 10);
            const updated = parseInt((await redis.get(progressKeys.updatedProducts(fk))) || 0, 10);
            const skipped = parseInt((await redis.get(progressKeys.skippedProducts(fk))) || 0, 10);
            const failed = parseInt((await redis.get(progressKeys.failedProducts(fk))) || 0, 10);
            const completed = updated + skipped + failed;
            out[fk] = { totalRows, updated, skipped, failed, completed };
          }
          return out;
        });
        alreadyRunning = isRunActiveFromProgress(progress);
      }

      if (alreadyRunning) {
        return res.status(409).json({
          success: false,
          error: "Processing is already running",
          environment: config.envLabel,
        });
      }

      const validation = validateCanStartFromMappings(mappings);
      if (!validation.ok) {
        return res.status(400).json({
          success: false,
          error: "Cannot start processing: missing required mappings",
          reasons: validation.reasons,
          environment: config.envLabel,
        });
      }

      // For quantity-only mode, check that all ready files have quantity mapping
      const readyFiles = (mappings.files || []).filter(f => f.status === "ready" || 
        (f.status === "pending" && f.mapping && typeof f.mapping === "object" &&
          f.mapping.partNumber?.trim() && f.mapping.manufacturer?.trim() && f.mapping.category?.trim()));
      
      const filesWithoutQuantity = readyFiles.filter(f => 
        !f.mapping?.quantity || !String(f.mapping.quantity).trim()
      );

      if (filesWithoutQuantity.length > 0) {
        const fileNames = filesWithoutQuantity.map(f => f.fileKey).join(', ');
        return res.status(400).json({
          success: false,
          error: "Quantity-only mode requires quantity column mapping",
          reasons: [`Missing quantity mapping for: ${fileNames}`],
          environment: config.envLabel,
        });
      }

      // Auto-promote pending files with complete mappings
      let promoted = 0;
      const now = new Date().toISOString();
      for (const f of mappings.files || []) {
        if (f && f.status === "pending" && f.mapping && typeof f.mapping === "object") {
          const mapped =
            typeof f.mapping.partNumber === "string" && f.mapping.partNumber.trim() &&
            typeof f.mapping.manufacturer === "string" && f.mapping.manufacturer.trim() &&
            typeof f.mapping.category === "string" && f.mapping.category.trim();
          if (mapped) {
            f.status = "ready";
            f.updatedAt = now;
            promoted += 1;
          }
        }
      }

      if (promoted > 0) {
        writeMappings(config.paths.mappingsPath, mappings);
      }

      // For quantity-only mode, clear progress and checkpoints for all ready files
      // This ensures files get re-processed even if they were previously completed
      const filesToProcess = (mappings.files || []).filter(f => f.status === "ready");
      let progressCleared = 0;
      
      for (const file of filesToProcess) {
        const fileKey = file.fileKey;
        try {
          // Clear Redis progress
          await withRedis(config.redisUrl, async (redis) => {
            const keys = progressKeys.allKeys(fileKey);
            await redis.del([
              keys.totalRows,
              keys.updated,
              keys.skipped,
              keys.failed,
              keys.processing,
            ]);
          });

          // Clear checkpoint entries
          for (const checkpointPath of config.paths.checkpointPaths || []) {
            try {
              if (!checkpointPath || !fs.existsSync(checkpointPath)) continue;
              const raw = fs.readFileSync(checkpointPath, "utf-8") || "{}";
              const json = JSON.parse(raw);
              if (json && Object.prototype.hasOwnProperty.call(json, fileKey)) {
                delete json[fileKey];
                fs.writeFileSync(checkpointPath, JSON.stringify(json, null, 2));
              }
            } catch (e) {
              console.error(`[trigger/quantity-only] Failed to clear checkpoint for ${fileKey}: ${e.message}`);
            }
          }
          
          progressCleared++;
          console.log(`[trigger/quantity-only] Cleared progress for: ${fileKey}`);
        } catch (e) {
          console.error(`[trigger/quantity-only] Failed to clear progress for ${fileKey}: ${e.message}`);
        }
      }

      console.log(`[trigger] [${config.envLabel}] Triggering QUANTITY-ONLY processing (cleared ${progressCleared} file(s) progress)...`);
      await restartAppQuantityOnly();
      res.json({
        success: true,
        message: `Quantity-only processing triggered (cleared progress for ${progressCleared} file(s))`,
        mode: "quantity",
        environment: config.envLabel,
      });
    } catch (err) {
      console.error(`[trigger/quantity-only] Error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Run status (mapping readiness + running indicator)
  router.get("/run-status", async (req, res) => {
    try {
      const mappings = readMappings(config.paths.mappingsPath);

      const progress = await withRedis(config.redisUrl, async (redis) => {
        const fileKeys = await redis.keys(progressKeys.totalRowsPattern());
        const out = {};

        for (const key of fileKeys) {
          const fk = progressKeys.extractFileKey(key);
          const totalRows = parseInt((await redis.get(progressKeys.totalRows(fk))) || 0, 10);
          const updated = parseInt((await redis.get(progressKeys.updatedProducts(fk))) || 0, 10);
          const skipped = parseInt((await redis.get(progressKeys.skippedProducts(fk))) || 0, 10);
          const failed = parseInt((await redis.get(progressKeys.failedProducts(fk))) || 0, 10);
          const completed = updated + skipped + failed;
          out[fk] = {
            totalRows,
            updated,
            skipped,
            failed,
            completed,
            percentage: totalRows > 0 ? Math.round((completed / totalRows) * 100) : 0,
          };
        }

        return out;
      });

      let running = false;
      try {
        const counts = await getQueueActivityCounts();
        running = isQueueRunning(counts);
      } catch {
        running = isRunActiveFromProgress(progress);
      }
      const validation = validateCanStartFromMappings(mappings);

      res.json({
        environment: config.envLabel,
        running,
        canStart: validation.ok && !running,
        reasons: running ? ["Processing is currently running"] : validation.reasons,
        readyFiles: (validation.startCandidates || []).map((f) => ({
          fileKey: f.fileKey,
          status: f.status,
          mapping: f.mapping || null,
          headers: f.headers || [],
        })),
        progress,
        timestamp: new Date().toISOString(),
      });
    } catch (err) {
      console.error("[run-status] Error:", err);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Progress
  router.get("/progress", async (req, res) => {
    try {
      // Get queue activity counts for job-level visibility
      let queueCounts = { waiting: 0, active: 0, delayed: 0 };
      try {
        queueCounts = await getQueueActivityCounts();
      } catch (e) {
        console.error(`[progress] Failed to get queue counts: ${e.message}`);
      }

      const progress = await withRedis(config.redisUrl, async (redis) => {
        const fileKeys = await redis.keys(progressKeys.totalRowsPattern());
        const out = {};

        for (const key of fileKeys) {
          const fileKey = progressKeys.extractFileKey(key);

          const totalRows = parseInt((await redis.get(progressKeys.totalRows(fileKey))) || 0, 10);
          const updated = parseInt(
            (await redis.get(progressKeys.updatedProducts(fileKey))) || 0,
            10
          );
          const skipped = parseInt(
            (await redis.get(progressKeys.skippedProducts(fileKey))) || 0,
            10
          );
          const failed = parseInt(
            (await redis.get(progressKeys.failedProducts(fileKey))) || 0,
            10
          );
          const processing = parseInt(
            (await redis.get(progressKeys.processingProducts(fileKey))) || 0,
            10
          );

          const completed = updated + skipped + failed;

          out[fileKey] = {
            totalRows,
            updated,
            skipped,
            failed,
            processing,
            completed,
            percentage:
              totalRows > 0 ? Math.round((completed / totalRows) * 100) : 0,
          };
        }

        return out;
      });

      res.json({
        progress,
        queue: queueCounts,
        environment: config.envLabel,
      });
    } catch (err) {
      console.error(`[progress] Error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // Remove progress for a single fileKey
  router.delete("/progress/:fileKey", async (req, res) => {
    try {
      const fileKey = decodeURIComponent(req.params.fileKey);

      const deleted = await withRedis(config.redisUrl, async (redis) => {
        const keys = progressKeys.allKeys(fileKey);
        return await redis.del([
          keys.totalRows,
          keys.updated,
          keys.skipped,
          keys.failed,
          keys.processing,
        ]);
      });

      // Also clear checkpoint entries (so resume logic doesn't re-hydrate progress)
      let checkpointCleared = 0;
      for (const checkpointPath of config.paths.checkpointPaths || []) {
        try {
          if (!checkpointPath || !fs.existsSync(checkpointPath)) continue;
          const raw = fs.readFileSync(checkpointPath, "utf-8") || "{}";
          const json = JSON.parse(raw);
          if (json && Object.prototype.hasOwnProperty.call(json, fileKey)) {
            delete json[fileKey];
            fs.writeFileSync(checkpointPath, JSON.stringify(json, null, 2));
            checkpointCleared += 1;
          }
        } catch (e) {
          console.error(`[progress] Failed to clear checkpoint for ${fileKey}: ${e.message}`);
        }
      }

      res.json({
        success: true,
        fileKey,
        deletedKeys: deleted,
        checkpointFilesUpdated: checkpointCleared,
      });
    } catch (err) {
      console.error(`[progress:delete] Error: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Missing Products: List folders with missing products
  router.get("/missing-products", (req, res) => {
    try {
      const rootDir = config.paths.rootDir || path.join(__dirname, "../..");
      const folders = listMissingProductFolders(rootDir);
      res.json({ success: true, folders });
    } catch (err) {
      console.error(`[missing-products] Error listing: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Missing Products: Get details for a specific CSV folder
  router.get("/missing-products/:folderName", (req, res) => {
    try {
      const { folderName } = req.params;
      const missingProductsDir = path.join(config.paths.rootDir || path.join(__dirname, "../.."), "missing-products", folderName);
      
      if (!fs.existsSync(missingProductsDir)) {
        return res.status(404).json({ error: `Folder not found: ${folderName}` });
      }
      
      const categoryFolders = fs.readdirSync(missingProductsDir)
        .filter(name => name.startsWith("missing-") && fs.statSync(path.join(missingProductsDir, name)).isDirectory());
      
      const categories = categoryFolders.map(catFolder => {
        const jsonPath = path.join(missingProductsDir, catFolder, "missing_products.json");
        let products = [];
        if (fs.existsSync(jsonPath)) {
          try {
            products = JSON.parse(fs.readFileSync(jsonPath, "utf8"));
          } catch (e) { /* ignore */ }
        }
        return {
          name: catFolder.replace("missing-", ""),
          count: products.length,
          products: products.slice(0, 10) // Preview first 10
        };
      });
      
      res.json({ success: true, folderName, categories });
    } catch (err) {
      console.error(`[missing-products] Error getting details: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  // ---- Missing Products: Create missing products for a CSV folder
  router.post("/missing-products/:folderName/create", async (req, res) => {
    try {
      const { folderName } = req.params;
      
      console.log(`[missing-products] Starting creation for ${folderName}...`);
      
      const results = await processMissingProducts(folderName);
      
      console.log(`[missing-products] Completed: ${results.created} created, ${results.failed} failed`);
      
      res.json({ 
        success: true, 
        folderName,
        created: results.created,
        failed: results.failed,
        categories: results.categories
      });
    } catch (err) {
      console.error(`[missing-products] Error creating: ${err.message}`);
      res.status(500).json({ error: err.message });
    }
  });

  return router;
}

module.exports = {
  createApiRouter,
};
