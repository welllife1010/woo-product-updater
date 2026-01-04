/**
 * Orchestration layer for reading CSV files from S3 and enqueuing jobs.
 * Keeps the same behavior as the original src/services/s3-helpers.js.
 */

const { logErrorToFile, logInfoToFile, logUpdatesToFile } = require("../../utils/logger");

const { getReadyCsvFiles, getMappingForFile, markFileAsCompleted } = require("../csv-mapping-store");

const { getTotalRowsFromS3, getTotalRowsFromS3Streaming } = require("../csv/csv-row-counter");
const { checkExistingJobs, isFileFullyProcessed, initializeFileTracking } = require("./file-processing-state");
const { getLastProcessedRowAsync } = require("../../batch/checkpoint");
const { getLatestFolderKey, listObjectsV2, getObjectAsString, getObjectStream } = require("../s3/s3-objects");
const { enqueueBatchesFromCsvString, enqueueBatchesFromCsvStream } = require("./batch-job-enqueuer");

const USE_S3_STREAMING = String(process.env.USE_S3_STREAMING || "false").toLowerCase() === "true";

const readCSVAndEnqueueJobs = async (bucketName, key, batchSize) => {
  logInfoToFile(
    `🚀 readCSVAndEnqueueJobs() called: bucket=${bucketName}, key=${key}, batchSize=${batchSize}`
  );

  const mappingEntry = getMappingForFile(key);
  const mapping = mappingEntry?.mapping || null;

  if (!mapping) {
    logInfoToFile(
      `⚠️ No column mapping found for ${key}. ` +
        `Relying on normalized headers (part_number, category, manufacturer).`
    );
  } else {
    logInfoToFile(
      `✅ Using column mapping for ${key}: ` +
        `partNumber="${mapping.partNumber}", ` +
        `category="${mapping.category}", ` +
        `manufacturer="${mapping.manufacturer}"`
    );
  }

  const totalRows = USE_S3_STREAMING
    ? await getTotalRowsFromS3Streaming(bucketName, key)
    : await getTotalRowsFromS3(bucketName, key);

  if (totalRows === null || totalRows <= 0) {
    logErrorToFile(`❌ Skipping ${key}: Could not count rows or file is empty.`);
    return;
  }

  logInfoToFile(`📊 Total data rows in ${key}: ${totalRows}`);

  try {
    const alreadyInQueue = await checkExistingJobs(key);
    if (alreadyInQueue) {
      logInfoToFile(`⚠️ Skipping ${key}: Jobs already in queue.`);
      return;
    }
  } catch (error) {
    logErrorToFile(`❌ Error checking queue: ${error.message}`);
    return;
  }

  try {
    const fileProcessed = isFileFullyProcessed(key);
    if (fileProcessed) {
      logInfoToFile(`✅ Skipping ${key}: Already fully processed.`);
      markFileAsCompleted(key);
      return;
    }
  } catch (error) {
    logErrorToFile(`❌ Error checking processing status: ${error.message}`);
    return;
  }

  await initializeFileTracking(key, totalRows);

  // Get checkpoint for resume - this is the source of truth for where to resume.
  // NOTE: 0 is a valid value meaning "start from beginning", so we don't fall back
  // to job scanning which can return incorrect values from other environments.
  let resumeFromRow = 0;
  try {
    resumeFromRow = await getLastProcessedRowAsync(key);
    logInfoToFile(`📍 Checkpoint value for ${key}: ${resumeFromRow}`);
  } catch (error) {
    logErrorToFile(
      `⚠️ Failed to read checkpoint for ${key}: ${error.message}. Starting from row 0.`
    );
    resumeFromRow = 0;
  }

  if (isNaN(resumeFromRow) || resumeFromRow < 0) {
    logInfoToFile(`⚠️ Invalid resumeFromRow (${resumeFromRow}), resetting to 0.`);
    resumeFromRow = 0;
  }

  // Clamp resume point to file size.
  if (resumeFromRow > totalRows) {
    logInfoToFile(
      `⚠️ resumeFromRow (${resumeFromRow}) > totalRows (${totalRows}) for ${key}. Clamping.`
    );
    resumeFromRow = totalRows;
  }

  if (resumeFromRow >= totalRows) {
    logInfoToFile(`✅ All rows in ${key} already processed. Nothing to enqueue.`);
    markFileAsCompleted(key);
    return;
  }

  logInfoToFile(
    `🚀 Processing ${key} | Resuming from row ${resumeFromRow} | Total: ${totalRows}`
  );

  try {
    if (USE_S3_STREAMING) {
      const csvStream = await getObjectStream(bucketName, key);
      await enqueueBatchesFromCsvStream({
        csvStream,
        fileKey: key,
        totalRows,
        batchSize,
        mapping,
        resumeFromRow,
      });
    } else {
      const bodyContent = await getObjectAsString(bucketName, key);
      await enqueueBatchesFromCsvString({
        bodyContent,
        fileKey: key,
        totalRows,
        batchSize,
        mapping,
        resumeFromRow,
      });
    }
  } catch (error) {
    logErrorToFile(`❌ Error streaming CSV ${key}: ${error.message}`, error.stack);
  }
};

const processReadyCsvFilesFromMappings = async (bucketName, batchSize) => {
  try {
    const readyFiles = getReadyCsvFiles();

    if (!readyFiles.length) {
      logInfoToFile(
        "No READY CSV files found in csv-mappings.json. Nothing to process."
      );
      return;
    }

    logInfoToFile(`Found ${readyFiles.length} READY CSV files to process.`);

    for (const fileEntry of readyFiles) {
      const fileKey = fileEntry.fileKey;
      logInfoToFile(`🔄 Processing: ${fileKey}`);

      try {
        await readCSVAndEnqueueJobs(bucketName, fileKey, batchSize);
      } catch (error) {
        logErrorToFile(`❌ Error processing ${fileKey}: ${error.message}`, error.stack);
      }
    }

    logUpdatesToFile("✅ All READY CSV files have been processed.");
  } catch (error) {
    logErrorToFile(
      `❌ Error in processReadyCsvFilesFromMappings: ${error.message}`,
      error.stack
    );
  }
};

const processCSVFilesInS3LatestFolder = async (bucketName, batchSize) => {
  try {
    const latestFolder = await getLatestFolderKey(bucketName);

    if (!latestFolder) {
      logErrorToFile(`No folders found in bucket: ${bucketName}`);
      return;
    }

    logInfoToFile(`📂 Processing files in latest folder: ${latestFolder}`);

    const listData = await listObjectsV2({ bucketName, prefix: latestFolder });

    if (!listData.Contents) {
      logErrorToFile(`No files found in folder: ${latestFolder}`);
      return;
    }

    const csvFiles = listData.Contents.filter((file) =>
      file.Key.toLowerCase().endsWith(".csv")
    );

    logInfoToFile(`Found ${csvFiles.length} CSV files in ${latestFolder}`);

    if (csvFiles.length === 0) {
      logErrorToFile(`No CSV files found in folder: ${latestFolder}`);
      return;
    }

    const processingTasks = csvFiles.map(async (file) => {
      try {
        logInfoToFile(`🔄 Processing: ${file.Key}`);
        await readCSVAndEnqueueJobs(bucketName, file.Key, batchSize);
      } catch (error) {
        logErrorToFile(`❌ Error processing ${file.Key}: ${error.message}`, error.stack);
      }
    });

    await Promise.all(processingTasks);
    logUpdatesToFile("✅ All CSV files in latest folder have been processed.");
  } catch (error) {
    logErrorToFile(
      `❌ Error in processCSVFilesInS3LatestFolder: ${error.message}`,
      error.stack
    );
  }
};

module.exports = {
  readCSVAndEnqueueJobs,
  processReadyCsvFilesFromMappings,
  processCSVFilesInS3LatestFolder,
};
