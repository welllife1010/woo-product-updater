const { exec } = require("child_process");

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
  stopWorkersIgnoreErrors,
  startWorkersIgnoreErrors,
};
