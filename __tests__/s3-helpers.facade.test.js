/**
 * Contract test: src/services/s3-helpers.js is a compatibility facade.
 *
 * We mock queue/redis so tests do NOT require a running Redis instance.
 */

jest.mock("../src/services/queue", () => {
  const getJobs = jest.fn().mockResolvedValue([]);

  return {
    batchQueue: {
      getJobs,
    },
    appRedis: {
      mSet: jest.fn().mockResolvedValue(true),
      quit: jest.fn().mockResolvedValue(true),
    },
  };
});

describe("s3-helpers compatibility facade", () => {
  test("exports the expected API surface", () => {
    const s3Helpers = require("../src/services/s3-helpers");

    const expectedKeys = [
      "getLatestFolderKey",
      "processCSVFilesInS3LatestFolder",
      "processReadyCsvFilesFromMappings",
      "readCSVAndEnqueueJobs",
      "normalizeHeaderKey",
      "getTotalRowsFromS3",
      "checkExistingJobs",
      "isFileFullyProcessed",
      "initializeFileTracking",
    ];

    for (const key of expectedKeys) {
      expect(s3Helpers).toHaveProperty(key);
      expect(typeof s3Helpers[key]).toBe("function");
    }
  });

  test("re-exports are wired to the split modules", () => {
    const s3Helpers = require("../src/services/s3-helpers");

    const s3Objects = require("../src/services/s3/s3-objects");
    const csvUtils = require("../src/services/csv/csv-utils");
    const rowCounter = require("../src/services/csv/csv-row-counter");
    const state = require("../src/services/ingest/file-processing-state");
    const orchestrator = require("../src/services/ingest/process-s3-csv");

    expect(s3Helpers.getLatestFolderKey).toBe(s3Objects.getLatestFolderKey);

    expect(s3Helpers.readCSVAndEnqueueJobs).toBe(orchestrator.readCSVAndEnqueueJobs);
    expect(s3Helpers.processReadyCsvFilesFromMappings).toBe(orchestrator.processReadyCsvFilesFromMappings);
    expect(s3Helpers.processCSVFilesInS3LatestFolder).toBe(orchestrator.processCSVFilesInS3LatestFolder);

    expect(s3Helpers.normalizeHeaderKey).toBe(csvUtils.normalizeHeaderKey);
    expect(s3Helpers.getTotalRowsFromS3).toBe(rowCounter.getTotalRowsFromS3);

    expect(s3Helpers.checkExistingJobs).toBe(state.checkExistingJobs);
    expect(s3Helpers.isFileFullyProcessed).toBe(state.isFileFullyProcessed);
    expect(s3Helpers.initializeFileTracking).toBe(state.initializeFileTracking);
  });
});
