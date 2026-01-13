const { exec } = require("child_process");
const { setUpdateMode, resetUpdateMode } = require("../../src/config/update-mode");

function execCommand(cmd) {
  return new Promise((resolve, reject) => {
    exec(cmd, (error, stdout, stderr) => {
      if (error) {
        error.stdout = stdout;
        error.stderr = stderr;
        return reject(error);
      }
      resolve({ stdout, stderr });
    });
  });
}

async function restartWorkers() {
  return execCommand("pm2 restart woo-update-app woo-worker");
}

async function restartApp() {
  return execCommand("pm2 restart woo-update-app");
}

/**
 * Restart app with UPDATE_MODE set to quantity-only.
 * Writes to runtime config file so workers pick it up on next batch.
 */
async function restartAppQuantityOnly() {
  // Set mode in config file (workers read this on each batch)
  setUpdateMode('quantity');
  console.log('[PM2] Set UPDATE_MODE=quantity in runtime config');
  return execCommand("pm2 restart woo-update-app woo-worker");
}

/**
 * Restart app with UPDATE_MODE set to full (default).
 * Resets to full update mode.
 */
async function restartAppFullMode() {
  // Reset mode in config file
  resetUpdateMode();
  console.log('[PM2] Reset UPDATE_MODE=full in runtime config');
  return execCommand("pm2 restart woo-update-app woo-worker");
}

async function stopWorkersIgnoreErrors() {
  try {
    await execCommand("pm2 stop woo-update-app woo-worker 2>/dev/null");
  } catch {
    // intentionally ignore
  }
}

async function startWorkersIgnoreErrors() {
  try {
    await execCommand("pm2 start woo-update-app woo-worker 2>/dev/null");
  } catch {
    // intentionally ignore
  }
}

module.exports = {
  execCommand,
  restartWorkers,
  restartApp,
  restartAppQuantityOnly,
  restartAppFullMode,
  stopWorkersIgnoreErrors,
  startWorkersIgnoreErrors,
};
