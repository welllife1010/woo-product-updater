// csv-mapping-server.js
//
// Small Express server that provides:
//  - API for listing CSV mappings
//  - API for saving column mappings
//  - API for uploading CSVs (stores in S3 + registers header mapping)
//  - Static HTML UI for admin users to interact with these APIs.

const express = require("express");
const fs = require("fs");
const path = require("path");
const multer = require("multer");
const { S3Client, PutObjectCommand } = require("@aws-sdk/client-s3");
const csvParser = require("csv-parser");

const app = express();
const PORT = process.env.CSV_MAPPING_PORT || 4000;
const MAPPINGS_PATH = path.join(__dirname, "csv-mappings.json");

const S3_BUCKET_NAME = process.env.S3_BUCKET_NAME; // reuse your existing env
const AWS_REGION = process.env.AWS_REGION || "us-west-2";

const s3Client = new S3Client({ region: AWS_REGION });

// For handling file uploads to the server
const upload = multer({ dest: path.join(__dirname, "tmp-uploads") });

app.use(express.json());

// ---------------------- helper: mapping store ----------------------

function loadMappings() {
  if (!fs.existsSync(MAPPINGS_PATH)) {
    return { files: [] };
  }
  return JSON.parse(fs.readFileSync(MAPPINGS_PATH, "utf8"));
}

function saveMappings(store) {
  fs.writeFileSync(MAPPINGS_PATH, JSON.stringify(store, null, 2));
}

// read header row from a local CSV path
async function getHeadersForCsv(localPath) {
  return new Promise((resolve, reject) => {
    fs.createReadStream(localPath)
      .pipe(csvParser())
      .on("headers", (headers) => {
        resolve(headers);
      })
      .on("error", reject);
  });
}

// Register a new fileKey + headers in csv-mappings.json
async function registerNewCsv(fileKey, localPath) {
  const headers = await getHeadersForCsv(localPath);

  const store = loadMappings();
  const exists = store.files.find((f) => f.fileKey === fileKey);
  if (!exists) {
    store.files.push({
      fileKey,
      status: "pending",
      headers,
      mapping: null,
      uploadedAt: new Date().toISOString(),
    });
    saveMappings(store);
  }
}

// ---------------------- API: mappings ----------------------

// GET /api/csv-mappings
// List all files + statuses.
app.get("/api/csv-mappings", (req, res) => {
  const store = loadMappings();
  res.json(store.files);
});

// GET /api/csv-mappings/:fileKey
// Return one file (with headers + current mapping).
app.get("/api/csv-mappings/:fileKey", (req, res) => {
  const fileKey = req.params.fileKey;
  const store = loadMappings();
  const entry = store.files.find((f) => f.fileKey === fileKey);
  if (!entry) {
    return res.status(404).json({ error: "Not found" });
  }
  res.json(entry);
});

// POST /api/csv-mappings/:fileKey
// Body: { partNumber, category, manufacturer }
app.post("/api/csv-mappings/:fileKey", (req, res) => {
  const fileKey = req.params.fileKey;
  const { partNumber, category, manufacturer } = req.body;

  if (!partNumber || !category || !manufacturer) {
    return res
      .status(400)
      .json({ error: "partNumber, category, manufacturer are required." });
  }

  const store = loadMappings();
  const entry = store.files.find((f) => f.fileKey === fileKey);
  if (!entry) {
    return res.status(404).json({ error: "File not found" });
  }

  entry.mapping = { partNumber, category, manufacturer };
  entry.status = "ready";
  saveMappings(store);

  res.json({ ok: true, entry });
});

// ---------------------- API: upload CSV ----------------------
//
// POST /api/upload-csv
//   form-data fields:
//     - file: (the CSV file)
//     - folder (optional): S3 folder prefix, e.g. "20250312/" or "ui-uploads/20250312/"
//
// Behavior:
//   1. Save file temporarily
//   2. Upload to S3 at: key = folder + originalName
//   3. Register new CSV with its header row and status "pending"
//   4. Return fileKey so batch system can use it later.

app.post(
  "/api/upload-csv",
  upload.single("file"),
  async (req, res) => {
    try {
      if (!req.file) {
        return res.status(400).json({ error: "No file uploaded." });
      }
      if (!S3_BUCKET_NAME) {
        return res
          .status(500)
          .json({ error: "S3_BUCKET_NAME env var is not set." });
      }

      // Determine S3 folder / prefix
      const userFolder = req.body.folder || ""; // may be empty
      // Default: ui-uploads/YYYYMMDD/
      let prefix;
      if (userFolder.trim()) {
        prefix = userFolder.trim();
      } else {
        const today = new Date();
        const y = today.getFullYear();
        const m = String(today.getMonth() + 1).padStart(2, "0");
        const d = String(today.getDate()).padStart(2, "0");
        prefix = `ui-uploads/${y}${m}${d}/`;
      }
      // Ensure prefix ends with '/'
      if (!prefix.endsWith("/")) {
        prefix += "/";
      }

      const originalName = req.file.originalname;
      const fileKey = `${prefix}${originalName}`;

      // Upload to S3
      const fileStream = fs.createReadStream(req.file.path);

      await s3Client.send(
        new PutObjectCommand({
          Bucket: S3_BUCKET_NAME,
          Key: fileKey,
          Body: fileStream,
          ContentType: "text/csv",
        })
      );

      // Register the CSV with its header row for mapping
      await registerNewCsv(fileKey, req.file.path);

      // Clean up temp file
      fs.unlinkSync(req.file.path);

      res.json({
        ok: true,
        fileKey,
        message: "File uploaded to S3 and registered for mapping.",
      });
    } catch (err) {
      console.error("Upload error:", err);
      res.status(500).json({ error: "Upload failed." });
    }
  }
);

// ---------------------- Static UI ----------------------

app.use(express.static(path.join(__dirname, "csv-mapping-ui")));

app.listen(PORT, () => {
  console.log(`CSV Mapping UI running on http://localhost:${PORT}`);
});
