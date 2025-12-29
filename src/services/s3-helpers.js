/**
 * Compatibility facade for legacy imports.
 *
 * The original implementation was split into smaller modules:
 * - src/services/s3/*         (S3 client + S3 operations)
 * - src/services/csv/*        (CSV config/utils/counter)
 * - src/services/ingest/*     (state checks + enqueue + orchestration)
 *
 * Public API remains unchanged.
 */

const { getLatestFolderKey } = require("./s3/s3-objects");
const {
  readCSVAndEnqueueJobs,
  processCSVFilesInS3LatestFolder,
  processReadyCsvFilesFromMappings,
} = require("./ingest/process-s3-csv");

const { normalizeHeaderKey } = require("./csv/csv-utils");
const { getTotalRowsFromS3 } = require("./csv/csv-row-counter");
const {
  checkExistingJobs,
  isFileFullyProcessed,
  initializeFileTracking,
} = require("./ingest/file-processing-state");

module.exports = {
  // Folder discovery
  getLatestFolderKey,

  // Main processing functions
  processCSVFilesInS3LatestFolder,
  processReadyCsvFilesFromMappings,
  readCSVAndEnqueueJobs,

  // Utility functions (exported for testing)
  normalizeHeaderKey,
  getTotalRowsFromS3,
  checkExistingJobs,
  isFileFullyProcessed,
  initializeFileTracking,
};
