const express = require("express");
const path = require("path");

const { createApiRouter } = require("./routes/api");
const { createAdminRouter } = require("./routes/admin");

function createUiApp(config) {
  const app = express();

  app.use(express.json());

  // Static UI
  app.use(express.static(config.paths.staticDir));

  // API
  app.use("/api", createApiRouter(config));
  app.use("/api/admin", createAdminRouter(config));

  // For SPAs / direct refreshes: serve index.html if it exists
  app.get("/", (req, res) => {
    res.sendFile(path.join(config.paths.staticDir, "index.html"));
  });

  return app;
}

module.exports = {
  createUiApp,
};
