// __tests__/csv-parse.test.js
const fs = require("fs");
const path = require("path");
const csvParser = require("csv-parser");

describe("CSV parsing with skipLines=9", () => {
  test("ignores first 9 lines and treats line 10 as header", async () => {
    const filePath = path.join(__dirname, "fixtures", "new-template.csv");

    const results = [];

    await new Promise((resolve, reject) => {
      fs.createReadStream(filePath)
        .pipe(csvParser({ skipLines: 9 }))
        .on("data", (row) => {
          results.push(row);
        })
        .on("end", resolve)
        .on("error", reject);
    });

    // We expect 2 data rows (ABC123, XYZ789)
    expect(results.length).toBe(2);

    // Check first row's keys are exactly the header labels from line 10
    const firstRow = results[0];
    expect(Object.keys(firstRow)).toEqual([
      "Manufacturer Part Number",
      "Product Description",
      "Packaging",
      "Stock Quantity",
      "Voltage",
      "Operating Temperature",
      "Supplier Device Package",
      "RoHS Compliance",
      "REACH Compliance",
      "HTS Code",
      "ECCN",
      "Moisture Sensitivity Level",
      "Tags",
    ]);

    // Check a couple of values
    expect(firstRow["Manufacturer Part Number"]).toBe("ABC123");
    expect(firstRow["Product Description"]).toBe("Small widget");

    const secondRow = results[1];
    expect(secondRow["Manufacturer Part Number"]).toBe("XYZ789");
    expect(secondRow["Product Description"]).toBe("Bigger widget");
  });
});
