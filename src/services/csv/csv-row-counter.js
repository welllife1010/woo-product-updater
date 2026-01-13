/**
 * CSV row counting utilities.
 * Uses csv-parser first (handles multiline quoted fields), then falls back.
 */

const { Readable } = require("stream");
const csvParser = require("csv-parser");

const { logErrorToFile, logInfoToFile } = require("../../utils/logger");
const { CSV_HEADER_ROW, CSV_SKIP_LINES } = require("./csv-config");
const { getObjectAsString, getObjectStream } = require("../s3/s3-objects");

const countCsvDataRowsFromString = async (bodyContent, { fileKeyForLogs } = {}) => {
  return new Promise((resolve) => {
    let rowCount = 0;

    const stream = Readable.from(bodyContent);

    stream
      .pipe(csvParser({ skipLines: CSV_SKIP_LINES }))
      .on("data", () => {
        rowCount++;
      })
      .on("end", () => {
        if (fileKeyForLogs) {
          logInfoToFile(`CSV ${fileKeyForLogs}: ${rowCount} data rows (csv-parser)`);
        }
        resolve(rowCount);
      })
      .on("error", (err) => {
        if (fileKeyForLogs) {
          logErrorToFile(`CSV parse error for ${fileKeyForLogs}: ${err.message}`);
        }

        // Fallback to line counting if csv-parser fails
        const lines = String(bodyContent)
          .split("\n")
          .filter((line) => {
            const trimmed = line.trim();
            return trimmed !== "" && trimmed !== "\ufeff" && !/^[\s\r\n]*$/.test(trimmed);
          });

        const dataRows = Math.max(0, lines.length - CSV_HEADER_ROW);
        if (fileKeyForLogs) {
          logInfoToFile(`CSV ${fileKeyForLogs}: ${dataRows} data rows (fallback line count)`);
        }
        resolve(dataRows);
      });
  });
};

const countCsvDataRowsFromStream = async (readStream, { fileKeyForLogs } = {}) => {
  return new Promise((resolve, reject) => {
    let rowCount = 0;

    readStream
      .pipe(csvParser({ skipLines: CSV_SKIP_LINES }))
      .on("data", () => {
        rowCount++;
      })
      .on("end", () => {
        if (fileKeyForLogs) {
          logInfoToFile(`CSV ${fileKeyForLogs}: ${rowCount} data rows (stream csv-parser)`);
        }
        resolve(rowCount);
      })
      .on("error", (err) => {
        // In streaming mode, we do not have a safe fallback line count (no full string).
        if (fileKeyForLogs) {
          logErrorToFile(`CSV parse error for ${fileKeyForLogs}: ${err.message}`);
        }
        reject(err);
      });
  });
};

const getTotalRowsFromS3 = async (bucketName, key) => {
  try {
    const bodyContent = await getObjectAsString(bucketName, key);
    return countCsvDataRowsFromString(bodyContent, { fileKeyForLogs: key });
  } catch (error) {
    logErrorToFile(`Error counting rows in ${key}: ${error.message}`);
    return null;
  }
};

/**
 * True streaming variant (no transformToString).
 * NOTE: This reads the S3 object once to count rows.
 */
const getTotalRowsFromS3Streaming = async (bucketName, key) => {
  try {
    const bodyStream = await getObjectStream(bucketName, key);
    return await countCsvDataRowsFromStream(bodyStream, { fileKeyForLogs: key });
  } catch (error) {
    logErrorToFile(`Error counting rows in ${key} (streaming): ${error.message}`);
    return null;
  }
};

module.exports = {
  countCsvDataRowsFromString,
  countCsvDataRowsFromStream,
  getTotalRowsFromS3,
  getTotalRowsFromS3Streaming,
};
