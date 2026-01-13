/**
 * Tests for post-refactor workflow correctness around:
 * - streaming ingest canonical key aliasing (part_number/manufacturer/category)
 * - batch mapping for meta_data keys (voltage/package) and deduping
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

describe("ingest aliasing via streaming batch enqueuer", () => {
  beforeEach(() => {
    jest.resetModules();
  });

  test("backfills canonical keys from common header aliases when no mapping is provided", async () => {
    const { addBatchJob } = require("../src/services/job-manager");
    const { enqueueBatchesFromCsvStream } = require("../src/services/ingest/batch-job-enqueuer");

    jest.spyOn(Date, "now").mockReturnValue(1700000000000);

    const csv =
      "MPN,Brand,Product Category\n" +
      "ABC-123,NXP,Microcontrollers\n" +
      "DEF-456,TI,Power Management\n";

    await enqueueBatchesFromCsvStream({
      csvStream: Readable.from(csv),
      fileKey: "file.csv",
      totalRows: 2,
      batchSize: 2,
      mapping: null,
      resumeFromRow: 0,
    });

    expect(addBatchJob).toHaveBeenCalledTimes(1);

    const jobData = addBatchJob.mock.calls[0][0];
    expect(jobData.batch).toHaveLength(2);

    expect(jobData.batch[0].part_number).toBe("ABC-123");
    expect(jobData.batch[0].manufacturer).toBe("NXP");
    expect(jobData.batch[0].category).toBe("Microcontrollers");

    expect(jobData.batch[1].part_number).toBe("DEF-456");
    expect(jobData.batch[1].manufacturer).toBe("TI");
    expect(jobData.batch[1].category).toBe("Power Management");

    Date.now.mockRestore();
  });

  test("explicit mapping is still honored for canonical keys", async () => {
    const { addBatchJob } = require("../src/services/job-manager");
    const { enqueueBatchesFromCsvStream } = require("../src/services/ingest/batch-job-enqueuer");

    jest.spyOn(Date, "now").mockReturnValue(1700000000000);

    const csv =
      "MPN,Part Number,Maker,Category\n" +
      "ALT-PN,REAL-PN,ACME,Widgets\n";

    await enqueueBatchesFromCsvStream({
      csvStream: Readable.from(csv),
      fileKey: "file.csv",
      totalRows: 1,
      batchSize: 10,
      mapping: {
        partNumber: "Part Number",
        manufacturer: "Maker",
        category: "Category",
      },
      resumeFromRow: 0,
    });

    expect(addBatchJob).toHaveBeenCalledTimes(1);
    const jobData = addBatchJob.mock.calls[0][0];

    expect(jobData.batch).toHaveLength(1);
    expect(jobData.batch[0].part_number).toBe("REAL-PN");
    expect(jobData.batch[0].manufacturer).toBe("ACME");
    expect(jobData.batch[0].category).toBe("Widgets");

    Date.now.mockRestore();
  });
});

describe("batch mapping createNewData meta keys", () => {
  test("maps normalized voltage/package fields into canonical meta keys and dedupes collisions", () => {
    const { createNewData } = require("../src/batch/map-new-data");

    const result = createNewData(
      {
        "Voltage / Supply": "3.3V",
        Voltage: "5V",
        "Package / Case": "QFN-32",
        manufacturer: "NXP",
        part_number: "ABC-123",
      },
      123,
      "FALLBACK-PN"
    );

    const meta = result.meta_data;

    const voltageEntries = meta.filter((m) => m.key === "voltage");
    expect(voltageEntries).toHaveLength(1);
    // Last-write-wins behavior should prefer `Voltage` over `Voltage / Supply`.
    expect(voltageEntries[0].value).toBe("5V");

    const packageEntries = meta.filter((m) => m.key === "package");
    expect(packageEntries).toHaveLength(1);
    expect(packageEntries[0].value).toBe("QFN-32");
  });

  test("sku fallback does not generate undefined/empty composite values", () => {
    const { createNewData } = require("../src/batch/map-new-data");

    const result = createNewData({}, 999, "PN123");

    expect(result.part_number).toBe("PN123");
    expect(result.sku).toBe("PN123");
  });
});
