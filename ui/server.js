const fs = require("fs");

const { createUiConfig } = require("./config");
const { createUiApp } = require("./app");

function start() {
  const config = createUiConfig(process.env);

  if (!fs.existsSync(config.paths.uploadDir)) {
    fs.mkdirSync(config.paths.uploadDir, { recursive: true });
  }

  console.log(
    `[csv-mapping-ui] Environment: ${config.envLabel} | S3 Bucket: ${config.s3BucketName}`
  );

  const app = createUiApp(config);

  app.listen(config.port, () => {
    console.log(
      `[${config.envLabel}] CSV Mapping UI running on http://localhost:${config.port}`
    );
  });

  return { app, config };
}

module.exports = {
  start,
};

// If invoked directly (node ui/server.js)
if (require.main === module) {
  start();
}
