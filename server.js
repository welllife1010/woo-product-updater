// UI server entrypoint (kept at repo root for PM2 + legacy scripts)
// Delegates to the refactored implementation in ui/server.js

require("./ui/server").start();
