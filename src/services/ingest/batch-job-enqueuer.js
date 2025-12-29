/**
 * Takes CSV content and enqueues batch jobs.
 * This module owns:
 * - streaming CSV parsing
 * - header normalization + optional mapping
 * - batching
 * - job id generation + duplicate check
 * - startIndex assignment (race-condition fix)
 */

const { pipeline } = require("stream");
const { promisify } = require("util");
const csvParser = require("csv-parser");

const { batchQueue } = require("../queue");
const { addBatchJob } = require("../job-manager");
const { createUniqueJobId } = require("../../utils/utils");
const {
  logErrorToFile,
  logInfoToFile,
  logUpdatesToFile,
} = require("../../utils/logger");

const { CSV_SKIP_LINES } = require("../csv/csv-config");
const { normalizeHeaderKey, normalizeRowKeys } = require("../csv/csv-utils");

// Minimal, safety-first aliasing so the rest of the pipeline can rely on
// canonical keys even when the user didn't provide a mapping.
function applyCommonAliases(normalizedData) {
  if (!normalizedData || typeof normalizedData !== "object") return normalizedData;

  // ---- Part number
  if (!normalizedData.part_number) {
    normalizedData.part_number =
      normalizedData.manufacturer_part_number ||
      normalizedData.mfr_part_number ||
      normalizedData.mpn ||
      normalizedData.partnumber ||
      // Some vendors incorrectly call it SKU; keep as last resort.
      normalizedData.sku ||
      "";
  }

  // ---- Manufacturer
  if (!normalizedData.manufacturer) {
    normalizedData.manufacturer =
      normalizedData.mfr ||
      normalizedData.mfg ||
      normalizedData.brand ||
      normalizedData.vendor ||
      normalizedData.supplier ||
      "";
  }

  // ---- Category
  if (!normalizedData.category) {
    normalizedData.category =
      normalizedData.product_category ||
      normalizedData.categories ||
      normalizedData.cat ||
      "";
  }

  return normalizedData;
}

const streamPipeline = promisify(pipeline);

const enqueueBatchesFromCsvStream = async ({
  csvStream,
  fileKey,
  totalRows,
  batchSize,
  mapping,
  resumeFromRow,
  /**
   * When true, do not enqueue jobs.
   * Instead, return the planned jobs (jobId/startIndex/batchSize) so callers
   * can compare streaming vs non-streaming behavior safely.
   */
  collectOnly = false,
}) => {
  let absoluteRowIndex = 0;

  let batch = [];
  let currentBatchStartIndex = 0;

  const plannedJobs = [];

  const existingJobs = collectOnly
    ? []
    : await batchQueue.getJobs([
        "waiting",
        "active",
        "delayed",
        "completed",
        "failed",
      ]);

  await streamPipeline(
    csvStream,
    csvParser({ skipLines: CSV_SKIP_LINES }),
    async function* (source) {
      for await (const chunk of source) {
        try {
          if (absoluteRowIndex < resumeFromRow) {
            absoluteRowIndex++;
            continue;
          }

          const normalizedData = normalizeRowKeys(chunk);

          // Provide best-effort canonical keys when the CSV doesn't use our
          // exact headers and no explicit mapping was configured.
          applyCommonAliases(normalizedData);

          if (mapping) {
            const partKeySafe = normalizeHeaderKey(mapping.partNumber);
            const categoryKeySafe = normalizeHeaderKey(mapping.category);
            const manufacturerKeySafe = normalizeHeaderKey(mapping.manufacturer);

            normalizedData.part_number = normalizedData[partKeySafe];
            normalizedData.category = normalizedData[categoryKeySafe];
            normalizedData.manufacturer = normalizedData[manufacturerKeySafe];
          }

          // Mapping should win, but still backfill any missing canonical keys
          // (helps with partial mappings).
          applyCommonAliases(normalizedData);

          if (batch.length === 0) {
            currentBatchStartIndex = absoluteRowIndex;
          }

          batch.push(normalizedData);

          if (batch.length >= batchSize) {
            const jobData = {
              batch: [...batch],
              fileKey,
              totalProductsInFile: totalRows,
              startIndex: currentBatchStartIndex,
              batchSize: batch.length,
            };

            const jobId = createUniqueJobId(
              fileKey,
              "s3-helper_readCSVAndEnqueueJobs",
              String(currentBatchStartIndex)
            );

            plannedJobs.push({
              jobId,
              fileKey,
              startIndex: currentBatchStartIndex,
              batchSize: batch.length,
              rangeStart: currentBatchStartIndex,
              rangeEnd: currentBatchStartIndex + batch.length - 1,
            });

            if (!collectOnly) {
              const isDuplicate = existingJobs.some((job) => job.id === jobId);
              if (isDuplicate) {
                logInfoToFile(`⚠️ Duplicate job ${jobId}, skipping.`);
              } else {
                try {
                  const job = await addBatchJob(jobData, jobId);
                  if (!job) {
                    throw new Error(`addBatchJob returned null for ${jobId}`);
                  }

                  logInfoToFile(
                    `✅ Job enqueued: ${job.id} | ` +
                      `Rows ${currentBatchStartIndex}-${
                        currentBatchStartIndex + batch.length - 1
                      } | ` +
                      `File: ${fileKey}`
                  );
                } catch (error) {
                  logErrorToFile(
                    `❌ Failed to enqueue job ${jobId}: ${error.message}`
                  );
                }
              }
            }

            batch = [];
          }

          absoluteRowIndex++;
        } catch (error) {
          logErrorToFile(
            `Error processing row ${absoluteRowIndex} in ${fileKey}: ${error.message}`
          );
          absoluteRowIndex++;
        }
      }

      if (batch.length > 0) {
        const jobData = {
          batch: [...batch],
          fileKey,
          totalProductsInFile: totalRows,
          startIndex: currentBatchStartIndex,
          batchSize: batch.length,
        };

        const jobId = createUniqueJobId(
          fileKey,
          "s3-helper_readCSVAndEnqueueJobs_FINAL",
          String(currentBatchStartIndex)
        );

        plannedJobs.push({
          jobId,
          fileKey,
          startIndex: currentBatchStartIndex,
          batchSize: batch.length,
          rangeStart: currentBatchStartIndex,
          rangeEnd: currentBatchStartIndex + batch.length - 1,
        });

        if (!collectOnly) {
          const isDuplicate = existingJobs.some((job) => job.id === jobId);
          if (isDuplicate) {
            logInfoToFile(`⚠️ Duplicate final job ${jobId}, skipping.`);
          } else {
            try {
              const job = await addBatchJob(jobData, jobId);
              if (!job) {
                throw new Error(
                  `addBatchJob returned null for final job ${jobId}`
                );
              }

              logInfoToFile(
                `✅ FINAL job enqueued: ${job.id} | ` +
                  `Rows ${currentBatchStartIndex}-${
                    currentBatchStartIndex + batch.length - 1
                  } | ` +
                  `File: ${fileKey}`
              );
            } catch (error) {
              logErrorToFile(
                `❌ Failed to enqueue final job ${jobId}: ${error.message}`
              );
            }
          }
        }
      }
    }
  );

  logUpdatesToFile(
    `✅ Completed reading ${fileKey}: ${absoluteRowIndex} rows processed into jobs`
  );

  return { absoluteRowIndex, plannedJobs };
};

const enqueueBatchesFromCsvString = async ({
  bodyContent,
  fileKey,
  totalRows,
  batchSize,
  mapping,
  resumeFromRow,
  collectOnly = false,
}) => {
  const { Readable } = require("stream");
  const dataStream = Readable.from(bodyContent);
  return enqueueBatchesFromCsvStream({
    csvStream: dataStream,
    fileKey,
    totalRows,
    batchSize,
    mapping,
    resumeFromRow,
    collectOnly,
  });
};

module.exports = {
  enqueueBatchesFromCsvStream,
  enqueueBatchesFromCsvString,
};
