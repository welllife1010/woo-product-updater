/**
 * Unit tests for the "true streaming" path.
 *
 * We test two things:
 * 1) Row counting from a stream (csv-parser handles multiline quoted fields)
 * 2) Enqueue batching from a stream preserves startIndex + resume logic
 */

// Ensure CSV header is on row 1 for tests (so we don't skip 9 lines by default).
process.env.CSV_HEADER_ROW = "1";

const { Readable } = require("stream");

jest.mock("../src/services/queue", () => {
  return {
    batchQueue: {
      getJobs: jest.fn().mockResolvedValue([]),
    },
    appRedis: {
      mSet: jest.fn().mockResolvedValue(true),
      quit: jest.fn().mockResolvedValue(true),
    },
  };
});

jest.mock("../src/services/job-manager", () => {
  return {
    addBatchJob: jest.fn().mockImplementation(async (_jobData, jobId) => ({ id: jobId })),
  };
});

describe("streaming CSV utilities", () => {
  beforeEach(() => {
    jest.resetModules();
  });

  test("countCsvDataRowsFromStream counts multiline quoted rows correctly", async () => {
    const { countCsvDataRowsFromStream } = require("../src/services/csv/csv-row-counter");

    const csv =
      "part_number,description\n" +
      "A1,\"hello\"\n" +
      "A2,\"line1\nline2\"\n" +
      "A3,\"bye\"\n";

    const count = await countCsvDataRowsFromStream(Readable.from(csv));
    expect(count).toBe(3);
  });
});

describe("streaming batch enqueuer", () => {
  beforeEach(() => {
    jest.resetModules();
  });

  test("enqueueBatchesFromCsvStream batches with correct startIndex", async () => {
    const { addBatchJob } = require("../src/services/job-manager");
    const { enqueueBatchesFromCsvStream } = require("../src/services/ingest/batch-job-enqueuer");

    // Make jobIds deterministic
    jest.spyOn(Date, "now").mockReturnValue(1700000000000);

    const csv =
      "part_number,manufacturer,category\n" +
      "P1,M1,C1\n" +
      "P2,M2,C2\n" +
      "P3,M3,C3\n" +
      "P4,M4,C4\n" +
      "P5,M5,C5\n";

    await enqueueBatchesFromCsvStream({
      csvStream: Readable.from(csv),
      fileKey: "file.csv",
      totalRows: 5,
      batchSize: 2,
      mapping: null,
      resumeFromRow: 0,
    });

    expect(addBatchJob).toHaveBeenCalledTimes(3);

    const calls = addBatchJob.mock.calls;
    expect(calls[0][0].startIndex).toBe(0);
    expect(calls[0][0].batchSize).toBe(2);

    expect(calls[1][0].startIndex).toBe(2);
    expect(calls[1][0].batchSize).toBe(2);

    expect(calls[2][0].startIndex).toBe(4);
    expect(calls[2][0].batchSize).toBe(1);

    Date.now.mockRestore();
  });

  test("enqueueBatchesFromCsvStream respects resumeFromRow", async () => {
    const { addBatchJob } = require("../src/services/job-manager");
    const { enqueueBatchesFromCsvStream } = require("../src/services/ingest/batch-job-enqueuer");

    jest.spyOn(Date, "now").mockReturnValue(1700000000000);

    const csv =
      "part_number\n" +
      "P1\n" +
      "P2\n" +
      "P3\n" +
      "P4\n" +
      "P5\n";

    addBatchJob.mockClear();

    await enqueueBatchesFromCsvStream({
      csvStream: Readable.from(csv),
      fileKey: "file.csv",
      totalRows: 5,
      batchSize: 2,
      mapping: null,
      resumeFromRow: 2,
    });

    // Rows 0-1 skipped; remaining rows: 2,3,4 => two batches: [2,3] and [4]
    expect(addBatchJob).toHaveBeenCalledTimes(2);
    expect(addBatchJob.mock.calls[0][0].startIndex).toBe(2);
    expect(addBatchJob.mock.calls[1][0].startIndex).toBe(4);

    Date.now.mockRestore();
  });
});
