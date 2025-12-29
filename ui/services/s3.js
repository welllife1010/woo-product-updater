const { S3Client, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { parse } = require("csv-parse/sync");

function createS3Client(region) {
  return new S3Client({ region });
}

async function streamToString(stream) {
  if (!stream) return "";
  const chunks = [];
  for await (const chunk of stream) chunks.push(Buffer.from(chunk));
  return Buffer.concat(chunks).toString("utf8");
}

function extractFirstLine(text) {
  if (!text) return "";
  // Handle Windows + Unix newlines.
  const idx = text.indexOf("\n");
  if (idx === -1) return text;
  return text.slice(0, idx).replace(/\r$/, "");
}

function parseCsvHeaderLine(line) {
  const headerLine = String(line || "").replace(/^\uFEFF/, ""); // strip UTF-8 BOM
  if (!headerLine.trim()) return [];

  // csv-parse can handle quoted headers with commas.
  const records = parse(headerLine, {
    relax_quotes: true,
    relax_column_count: true,
    skip_empty_lines: true,
  });

  const row = Array.isArray(records) && records.length ? records[0] : [];
  return (row || []).map((h) => String(h ?? "").trim());
}

async function getCsvHeadersFromS3({ s3Client, bucket, key, maxBytes = 65536 }) {
  if (!bucket) throw new Error("S3 bucket is not configured");
  if (!key) throw new Error("Missing S3 object key");

  // Fetch only the first chunk; enough for typical header rows.
  const resp = await s3Client.send(
    new GetObjectCommand({
      Bucket: bucket,
      Key: key,
      Range: `bytes=0-${Math.max(1, Number(maxBytes) || 65536) - 1}`,
    })
  );

  const text = await streamToString(resp.Body);
  const firstLine = extractFirstLine(text);
  return parseCsvHeaderLine(firstLine);
}

async function uploadCsvToS3({ s3Client, bucket, key, body }) {
  return s3Client.send(
    new PutObjectCommand({
      Bucket: bucket,
      Key: key,
      Body: body,
      ContentType: "text/csv",
    })
  );
}

module.exports = {
  createS3Client,
  uploadCsvToS3,
  getCsvHeadersFromS3,
};
