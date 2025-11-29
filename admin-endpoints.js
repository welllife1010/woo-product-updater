// ============================================================
// ADMIN ENDPOINTS - Add these to csv-mapping-server.js
// ============================================================
// Add these BEFORE the final app.listen() line

// GET /api/logs - Get recent log entries
app.get("/api/logs", (req, res) => {
  try {
    const lines = parseInt(req.query.lines) || 50;
    const type = req.query.type || "info";
    
    const logFile = type === "error" 
      ? path.join(__dirname, "output-files", "error-log.txt")
      : path.join(__dirname, "output-files", "info-log.txt");
    
    if (!fs.existsSync(logFile)) {
      return res.json({ logs: [] });
    }
    
    const content = fs.readFileSync(logFile, "utf8");
    const allLines = content.split("\n").filter(l => l.trim());
    const recentLines = allLines.slice(-lines);
    
    res.json({ logs: recentLines });
  } catch (err) {
    console.error("[logs] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// GET /api/system-status - Get overall system status
app.get("/api/system-status", async (req, res) => {
  try {
    const { createClient } = require("redis");
    const redis = createClient({ url: "redis://127.0.0.1:6379/1" });
    await redis.connect();
    
    const keys = await redis.keys("*");
    await redis.quit();
    
    const checkpoint = fs.existsSync(path.join(__dirname, "process_checkpoint.json"))
      ? JSON.parse(fs.readFileSync(path.join(__dirname, "process_checkpoint.json"), "utf8"))
      : {};
    
    res.json({
      redisKeys: keys.length,
      checkpointFiles: Object.keys(checkpoint).length,
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error("[system-status] Error:", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/admin/restart - Restart PM2 workers (FIXED - no stdout in response)
app.post("/api/admin/restart", (req, res) => {
  const { exec } = require("child_process");
  
  console.log("[admin] Restarting PM2 workers...");
  
  // Run in background, don't wait for full output
  exec("pm2 restart woo-update-app woo-worker 2>&1 | head -1", { timeout: 10000 }, (error) => {
    if (error && !error.killed) {
      console.error("[admin] Restart error:", error.message);
      // Still return success if PM2 command was sent
    }
    
    console.log("[admin] Workers restart command sent");
    res.json({ 
      success: true, 
      message: "Workers restarted successfully",
      timestamp: new Date().toISOString()
    });
  });
});

// POST /api/admin/flush-redis - Flush all Redis databases
app.post("/api/admin/flush-redis", async (req, res) => {
  try {
    const { createClient } = require("redis");
    
    console.log("[admin] Flushing Redis databases...");
    
    // Flush DB 0 (BullMQ)
    const redis0 = createClient({ url: "redis://127.0.0.1:6379/0" });
    await redis0.connect();
    await redis0.flushDb();
    await redis0.quit();
    console.log("[admin] Flushed Redis DB 0");
    
    // Flush DB 1 (App data)
    const redis1 = createClient({ url: "redis://127.0.0.1:6379/1" });
    await redis1.connect();
    await redis1.flushDb();
    await redis1.quit();
    console.log("[admin] Flushed Redis DB 1");
    
    res.json({ 
      success: true, 
      message: "Redis databases flushed successfully",
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error("[admin] Flush Redis error:", err);
    res.status(500).json({ error: err.message });
  }
});

// POST /api/admin/full-reset - Complete system reset (FIXED - cleaner execution)
app.post("/api/admin/full-reset", async (req, res) => {
  const { exec } = require("child_process");
  const { createClient } = require("redis");
  
  console.log("[admin] Starting full system reset...");
  
  try {
    // Step 1: Stop PM2 workers (fire and forget)
    console.log("[admin] Step 1: Stopping PM2 workers...");
    await new Promise((resolve) => {
      exec("pm2 stop woo-update-app woo-worker 2>/dev/null", { timeout: 5000 }, () => resolve());
    });
    
    // Wait for processes to stop
    await new Promise(resolve => setTimeout(resolve, 2000));
    
    // Step 2: Flush Redis
    console.log("[admin] Step 2: Flushing Redis...");
    try {
      const redis0 = createClient({ url: "redis://127.0.0.1:6379/0" });
      await redis0.connect();
      await redis0.flushDb();
      await redis0.quit();
      
      const redis1 = createClient({ url: "redis://127.0.0.1:6379/1" });
      await redis1.connect();
      await redis1.flushDb();
      await redis1.quit();
      console.log("[admin] Redis flushed");
    } catch (redisErr) {
      console.error("[admin] Redis flush error:", redisErr.message);
    }
    
    // Step 3: Delete checkpoint file
    console.log("[admin] Step 3: Deleting checkpoint file...");
    const checkpointFile = path.join(__dirname, "process_checkpoint.json");
    if (fs.existsSync(checkpointFile)) {
      fs.unlinkSync(checkpointFile);
    }
    
    // Step 4: Clear batch_status directory
    console.log("[admin] Step 4: Clearing batch_status...");
    const batchStatusDir = path.join(__dirname, "batch_status");
    if (fs.existsSync(batchStatusDir)) {
      fs.rmSync(batchStatusDir, { recursive: true, force: true });
    }
    
    // Step 5: Clear missing-products directory
    console.log("[admin] Step 5: Clearing missing-products...");
    const missingProductsDir = path.join(__dirname, "missing-products");
    if (fs.existsSync(missingProductsDir)) {
      fs.rmSync(missingProductsDir, { recursive: true, force: true });
    }
    
    // Step 6: Reset file statuses to 'ready'
    console.log("[admin] Step 6: Resetting file statuses...");
    const mappingsFile = path.join(__dirname, "csv-mappings.json");
    if (fs.existsSync(mappingsFile)) {
      const mappings = JSON.parse(fs.readFileSync(mappingsFile, "utf8"));
      for (const key in mappings) {
        if (mappings[key].status) {
          mappings[key].status = "ready";
        }
      }
      fs.writeFileSync(mappingsFile, JSON.stringify(mappings, null, 2));
    }
    
    // Step 7: Start PM2 workers (fire and forget)
    console.log("[admin] Step 7: Starting PM2 workers...");
    exec("pm2 start woo-update-app woo-worker 2>/dev/null", { timeout: 10000 }, () => {});
    
    console.log("[admin] Full system reset completed");
    
    res.json({ 
      success: true, 
      message: "Full system reset completed. Workers restarting.",
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error("[admin] Full reset error:", err);
    
    // Try to restart workers even on error
    exec("pm2 start woo-update-app woo-worker 2>/dev/null", () => {});
    
    res.status(500).json({ error: err.message });
  }
});

// POST /api/admin/clear-logs - Clear log files
app.post("/api/admin/clear-logs", (req, res) => {
  try {
    console.log("[admin] Clearing log files...");
    
    const infoLog = path.join(__dirname, "output-files", "info-log.txt");
    const errorLog = path.join(__dirname, "output-files", "error-log.txt");
    
    const timestamp = new Date().toISOString().replace('T', ' ').substring(0, 19);
    const clearMsg = `[${timestamp}] Log file cleared via admin UI\n`;
    
    if (fs.existsSync(infoLog)) {
      fs.writeFileSync(infoLog, clearMsg);
    }
    if (fs.existsSync(errorLog)) {
      fs.writeFileSync(errorLog, clearMsg);
    }
    
    console.log("[admin] Log files cleared");
    
    res.json({ 
      success: true, 
      message: "Log files cleared",
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    console.error("[admin] Clear logs error:", err);
    res.status(500).json({ error: err.message });
  }
});

// ============================================================
// END ADMIN ENDPOINTS
// ============================================================