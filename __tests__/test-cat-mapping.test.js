/**
 * @file __tests__/test-cat-mapping.test.js
 *
 * Purpose: Verify that the new LED CSV (test-cat.csv) maps
 * RoHS/REACH/HTS/ECCN/MSL fields into the correct ACF meta keys
 * using createNewData() from src/batch/map-new-data.js.
 */

const fs = require("fs");
const path = require("path");
const csvParser = require("csv-parser");

// ✅ Import the canonical mapper (NOT batch-helpers)
const { createNewData } = require("../src/batch/map-new-data");

// Helper: load the test CSV into JS objects
function loadTestCsv() {
  return new Promise((resolve, reject) => {
    const rows = [];
    const filePath = path.join(__dirname, "test-files", "test-cat.csv");

    fs.createReadStream(filePath)
      // Header row is on line 1 in this test CSV → no skipLines
      .pipe(csvParser())
      .on("data", (row) => rows.push(row))
      .on("end", () => resolve(rows))
      .on("error", reject);
  });
}

test("LED CSV maps RoHS/REACH/HTS/ECCN/MSL into proper meta_data keys", async () => {
  const rows = await loadTestCsv();

  // Make sure we actually loaded something
  expect(rows.length).toBeGreaterThan(0);

  // Just test the first row for now
  const row = rows[0];

  // ✅ Get the first column as the part number (since col 1 is always MPN)
  const headers = Object.keys(row);
  // Optional debug if you want to see what headers look like:
  // console.log("Row headers:", headers);

  const partNumberFromCsv = row[headers[0]];

  expect(partNumberFromCsv).toBe("LT9C83-43-940");

  // Call createNewData exactly like processBatch would:
  const newData = createNewData(row, 9999, partNumberFromCsv);

  // ✅ 1) part_number fallback should be correct
  expect(newData.part_number).toBe("LT9C83-43-940");

  // Helper to get a meta value by key from meta_data array
  const getMeta = (key) => {
    const found = newData.meta_data.find((m) => m.key === key);
    return found ? found.value : undefined;
  };

  // ✅ 2) RoHS / REACH / HTS / ECCN / MSL mappings
  expect(getMeta("rohs_status")).toBe("ROHS3 Compliant");
  expect(getMeta("reach_status")).toBe("REACH Unaffected");
  expect(getMeta("htsus_code")).toBe("8541.41.0000");
  // ECCN is empty in your CSV → should be "" (empty string)
  expect(getMeta("export_control_class_number")).toBe("");
  expect(getMeta("moisture_sensitivity_level")).toBe("2");
});
