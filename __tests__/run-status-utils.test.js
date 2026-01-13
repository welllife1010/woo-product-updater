const {
  validateCanStartFromMappings,
  isRunActiveFromProgress,
} = require("../ui/routes/run-status-utils");

describe("run-status-utils", () => {
  test("validateCanStartFromMappings blocks when no eligible files", () => {
    const result = validateCanStartFromMappings({ files: [] });
    expect(result.ok).toBe(false);
    expect(result.reasons.join(" ")).toMatch(/eligible to run/i);
  });

  test("validateCanStartFromMappings blocks when READY file missing mapping", () => {
    const result = validateCanStartFromMappings({
      files: [{ fileKey: "a.csv", status: "ready", mapping: null }],
    });
    expect(result.ok).toBe(false);
    expect(result.reasons.join(" ")).toMatch(/Missing required mapping/i);
    expect(result.reasons.join(" ")).toMatch(/a\.csv/);
  });

  test("validateCanStartFromMappings passes when READY file has required mapping", () => {
    const result = validateCanStartFromMappings({
      files: [
        {
          fileKey: "a.csv",
          status: "ready",
          mapping: { partNumber: "Part", manufacturer: "Mfr", category: "Category" },
        },
      ],
    });
    expect(result.ok).toBe(true);
    expect(result.reasons).toEqual([]);
    expect(result.startCandidates).toHaveLength(1);
  });

  test("validateCanStartFromMappings passes when PENDING file has required mapping", () => {
    const result = validateCanStartFromMappings({
      files: [
        {
          fileKey: "a.csv",
          status: "pending",
          mapping: { partNumber: "Part", manufacturer: "Mfr", category: "Category" },
        },
      ],
    });
    expect(result.ok).toBe(true);
    expect(result.reasons).toEqual([]);
    expect(result.startCandidates).toHaveLength(1);
  });

  test("isRunActiveFromProgress detects in-progress work", () => {
    expect(
      isRunActiveFromProgress({
        "a.csv": { totalRows: 10, completed: 3 },
      })
    ).toBe(true);

    expect(
      isRunActiveFromProgress({
        "a.csv": { totalRows: 10, completed: 10 },
      })
    ).toBe(false);

    expect(isRunActiveFromProgress({})).toBe(false);
  });
});
