/**
 * Compare job planning between:
 * - non-streaming (transformToString + Readable.from)
 * - true streaming (S3 Body stream)
 *
 * This is a DRY RUN: it does NOT enqueue jobs.
 * It prints planned job ranges and fails (exit 1) if the plans differ.
 *
 * Usage:
 *   node scripts/compare-s3-streaming.js --bucket <bucket> --key <s3Key> [--batchSize 20]
 */

const dotenv = require("dotenv");
dotenv.config();

const { getObjectAsString, getObjectStream } = require("../src/services/s3/s3-objects");
const { getTotalRowsFromS3, getTotalRowsFromS3Streaming } = require("../src/services/csv/csv-row-counter");
const { enqueueBatchesFromCsvString, enqueueBatchesFromCsvStream } = require("../src/services/ingest/batch-job-enqueuer");

function parseArgs(argv) {
  const args = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === "--bucket") args.bucket = argv[++i];
    else if (a === "--key") args.key = argv[++i];
    else if (a === "--batchSize") args.batchSize = Number(argv[++i]);
  }
  return args;
}

function formatPlan(plan) {
  return plan.map((p) => ({
    startIndex: p.startIndex,
    batchSize: p.batchSize,
    range: `${p.rangeStart}-${p.rangeEnd}`,
    jobId: p.jobId,
  }));
}

function diffPlans(a, b) {
  const diffs = [];
  if (a.length !== b.length) {
    diffs.push(`jobCount differs: ${a.length} vs ${b.length}`);
  }
  const n = Math.max(a.length, b.length);
  for (let i = 0; i < n; i++) {
    const pa = a[i];
    const pb = b[i];
    if (!pa || !pb) {
      diffs.push(`job[${i}] missing on one side`);
      continue;
    }
    if (pa.startIndex !== pb.startIndex || pa.batchSize !== pb.batchSize) {
      diffs.push(
        `job[${i}] differs: startIndex ${pa.startIndex} vs ${pb.startIndex}, batchSize ${pa.batchSize} vs ${pb.batchSize}`
      );
    }
  }
  return diffs;
}

(async () => {
  const { bucket, key, batchSize = 20 } = parseArgs(process.argv);

  if (!bucket || !key) {
    console.error("Missing required args: --bucket and --key");
    process.exit(2);
  }

  // Freeze time so jobIds are deterministic across both runs.
  const fixedNow = Date.now();
  const originalNow = Date.now;
  Date.now = () => fixedNow;

  try {
    console.log("Comparing plans for:", { bucket, key, batchSize });

    // -------- non-streaming plan --------
    const totalRowsA = await getTotalRowsFromS3(bucket, key);
    const body = await getObjectAsString(bucket, key);
    const resultA = await enqueueBatchesFromCsvString({
      bodyContent: body,
      fileKey: key,
      totalRows: totalRowsA,
      batchSize,
      mapping: null,
      resumeFromRow: 0,
      collectOnly: true,
    });

    // -------- streaming plan --------
    const totalRowsB = await getTotalRowsFromS3Streaming(bucket, key);
    const stream = await getObjectStream(bucket, key);
    const resultB = await enqueueBatchesFromCsvStream({
      csvStream: stream,
      fileKey: key,
      totalRows: totalRowsB,
      batchSize,
      mapping: null,
      resumeFromRow: 0,
      collectOnly: true,
    });

    console.log("totalRows:", { nonStreaming: totalRowsA, streaming: totalRowsB });

    const planA = formatPlan(resultA.plannedJobs);
    const planB = formatPlan(resultB.plannedJobs);

    const diffs = diffPlans(planA, planB);

    if (diffs.length === 0 && totalRowsA === totalRowsB) {
      console.log("✅ Plans match. Job ranges:");
      console.table(planA.map(({ startIndex, batchSize, range }) => ({ startIndex, batchSize, range })));
      process.exit(0);
    }

    console.log("❌ Plans differ!");
    diffs.forEach((d) => console.log(" -", d));

    console.log("\nNon-streaming plan:");
    console.table(planA.map(({ startIndex, batchSize, range }) => ({ startIndex, batchSize, range })));

    console.log("\nStreaming plan:");
    console.table(planB.map(({ startIndex, batchSize, range }) => ({ startIndex, batchSize, range })));

    process.exit(1);
  } catch (err) {
    console.error("❌ Comparison failed:", err);
    process.exit(1);
  } finally {
    Date.now = originalNow;
  }
})();
