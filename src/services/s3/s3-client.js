/**
 * S3 client setup (AWS SDK v3)
 * Kept intentionally small so S3 configuration changes don't affect CSV/job logic.
 */

const { S3Client } = require("@aws-sdk/client-s3");

const s3Client = new S3Client({
  region: process.env.AWS_REGION_NAME,
  endpoint: process.env.AWS_ENDPOINT_URL,
  forcePathStyle: true,
  requestTimeout: 300000,
});

module.exports = { s3Client };
