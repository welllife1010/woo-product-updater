const express = require("express");
const fs = require("fs");
const os = require("os");
const path = require("path");
const request = require("supertest");

jest.mock("../ui/services/redis", () => {
  return {
    withRedis: jest.fn(),
  };
});

const { withRedis } = require("../ui/services/redis");
const { createApiRouter } = require("../ui/routes/api");

function mkTmpDir() {
  return fs.mkdtempSync(path.join(os.tmpdir(), "woo-ui-test-"));
}

function createConfig({ checkpointPaths }) {
  const tmpDir = mkTmpDir();
  const outputDir = path.join(tmpDir, "output");
  const uploadDir = path.join(tmpDir, "uploads");

  return {
    port: 0,
    executionMode: "test",
    envLabel: "TEST",
    s3BucketName: "test-bucket",
    s3Region: "us-east-1",
    redisUrl: "redis://test",
    paths: {
      outputDir,
      uploadDir,
      staticDir: path.join(tmpDir, "static"),
      mappingsPath: path.join(tmpDir, "mappings.json"),
      checkpointPaths: checkpointPaths || [],
    },
  };
}

describe("DELETE /api/csv-mappings/:fileKey", () => {
  test("deletes mapping and clears Redis progress + checkpoint entry", async () => {
    const fileKey = "vendor/test-file.csv";

    const tmpDir = mkTmpDir();
    const checkpointPath = path.join(tmpDir, "process_checkpoint.json");
    fs.writeFileSync(
      checkpointPath,
      JSON.stringify({ [fileKey]: { row: 123 }, other: { row: 1 } }, null, 2)
    );

    const redis = {
      del: jest.fn(async (keys) => (Array.isArray(keys) ? keys.length : 1)),
      keys: jest.fn(async () => []),
      get: jest.fn(async () => null),
    };

    withRedis.mockImplementation(async (_redisUrl, fn) => fn(redis));

    const config = createConfig({ checkpointPaths: [checkpointPath] });

    // Seed mappings with the file.
    fs.writeFileSync(
      config.paths.mappingsPath,
      JSON.stringify({ files: [{ fileKey, status: "pending", mapping: null }] }, null, 2)
    );

    const app = express();
    app.use("/api", createApiRouter(config));

    const res = await request(app)
      .delete(`/api/csv-mappings/${encodeURIComponent(fileKey)}`)
      .expect(200);

    expect(res.body).toMatchObject({ success: true, fileKey });

    const updatedMappings = JSON.parse(fs.readFileSync(config.paths.mappingsPath, "utf-8"));
    expect(updatedMappings.files).toEqual([]);

    expect(redis.del).toHaveBeenCalledWith([
      `total-rows:${fileKey}`,
      `updated-products:${fileKey}`,
      `skipped-products:${fileKey}`,
      `failed-products:${fileKey}`,
    ]);

    const updatedCheckpoint = JSON.parse(fs.readFileSync(checkpointPath, "utf-8"));
    expect(updatedCheckpoint).not.toHaveProperty(fileKey);
    expect(updatedCheckpoint).toHaveProperty("other");
  });
});
