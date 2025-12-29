/**
 * S3 object operations (listing, folder discovery, downloads).
 * No CSV parsing and no queue/job logic in this module.
 */

const { ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { s3Client } = require("./s3-client");

const { logErrorToFile, logInfoToFile } = require("../../utils/logger");

const listObjectsV2 = async ({ bucketName, prefix, delimiter } = {}) => {
  const params = {
    Bucket: bucketName,
    ...(prefix ? { Prefix: prefix } : {}),
    ...(delimiter ? { Delimiter: delimiter } : {}),
  };
  return s3Client.send(new ListObjectsV2Command(params));
};

const getLatestFolderKey = async (bucketName) => {
  try {
    const data = await listObjectsV2({ bucketName, delimiter: "/" });

    if (!data.CommonPrefixes || data.CommonPrefixes.length === 0) {
      logErrorToFile(`No folders found in S3 bucket: ${bucketName}`);
      return null;
    }

    const folders = data.CommonPrefixes.map((prefix) => prefix.Prefix).sort();
    const latestFolder = folders[folders.length - 1];

    logInfoToFile(`Latest folder in ${bucketName}: ${latestFolder}`);
    return latestFolder;
  } catch (error) {
    logErrorToFile(`Error getting latest folder: ${error.message}`);
    return null;
  }
};

const getObjectAsString = async (bucketName, key) => {
  const params = { Bucket: bucketName, Key: key };
  const data = await s3Client.send(new GetObjectCommand(params));
  return data.Body.transformToString();
};

/**
 * Return the S3 object body as a readable stream.
 *
 * This is the "true streaming" building block: callers can pipe this
 * directly into parsers (csv-parser) without loading the entire file into memory.
 */
const getObjectStream = async (bucketName, key) => {
  const params = { Bucket: bucketName, Key: key };
  const data = await s3Client.send(new GetObjectCommand(params));
  return data.Body;
};

module.exports = {
  listObjectsV2,
  getLatestFolderKey,
  getObjectAsString,
  getObjectStream,
};
