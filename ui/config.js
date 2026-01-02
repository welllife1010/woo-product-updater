const fs = require("fs");
const path = require("path");

const {
  resolveAppEnv,
  getEnvLabel: getEnvLabelForAppEnv,
  getS3BucketName,
} = require("../src/config/runtime-env");

// Backward-compat: UI historically called this "executionMode".
// Under Option A, it is the resolved APP_ENV (production|staging|development).
function getEnvLabel(executionMode) {
  return getEnvLabelForAppEnv(executionMode);
}

function pickStaticDir(uiDir) {
  const candidates = [path.join(uiDir, "public"), path.join(uiDir, "csv-mapping-ui")];
  for (const dir of candidates) {
    if (fs.existsSync(dir) && fs.statSync(dir).isDirectory()) return dir;
  }
  // Default to ui/public even if it doesn't exist yet; Express will just 404 static
  return path.join(uiDir, "public");
}

function createUiConfig(env = process.env) {
  const uiDir = __dirname; // ui/
  const repoRoot = path.resolve(uiDir, "..");

  const executionMode = resolveAppEnv(env);
  const envLabel = getEnvLabel(executionMode);

  const port = Number(env.CSV_MAPPING_PORT || 4000);

  const s3Region = env.AWS_REGION || env.AWS_REGION_NAME || "us-west-1";

  const s3BucketName = getS3BucketName(env, executionMode);

  const paths = {
    repoRoot,
    uiDir,
    staticDir: pickStaticDir(uiDir),
    uploadDir: path.join(uiDir, "tmp-uploads"),

    mappingsPath: path.join(repoRoot, "csv-mappings.json"),
    outputDir: path.join(repoRoot, "output-files"),

    // Support both legacy and current checkpoint locations
    checkpointPaths: [
      path.join(repoRoot, "process_checkpoint.json"),
      path.join(repoRoot, "src", "batch", "process_checkpoint.json"),
    ],
  };

  return {
    executionMode,
    appEnv: executionMode,
    envLabel,
    port,
    s3Region,
    s3BucketName,
    paths,

    // Redis is hard-coded today; keep it centralized for future extraction
    redisUrl: env.REDIS_URL || "redis://127.0.0.1:6379/1",
  };
}

module.exports = {
  createUiConfig,
  getEnvLabel,
};
