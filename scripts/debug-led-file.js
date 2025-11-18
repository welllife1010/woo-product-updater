const fs = require("fs");
const path = require("path");
const csvParser = require("csv-parser");

const filePath = path.join(__dirname, "../LED-Emitters-IR-UV-Visible.csv");

(async () => {
  const rows = [];
  fs.createReadStream(filePath)
    // if header is on row 10:
    .pipe(csvParser({ skipLines: 9 }))
    // if header is on row 1, change to: .pipe(csvParser())
    .on("data", (row) => {
      rows.push(row);
      if (rows.length === 1) {
        // Show the raw headers and normalized keys
        const normalized = {};
        for (const key of Object.keys(row)) {
          const norm = key.trim().toLowerCase().replace(/\s+/g, "_");
          normalized[norm] = row[key];
        }
        console.log("Raw keys:", Object.keys(row));
        console.log("Normalized keys:", Object.keys(normalized));
      }
    })
    .on("end", () => {
      console.log(`Loaded ${rows.length} data rows from LED file.`);
    })
    .on("error", (err) => {
      console.error("Error reading LED file:", err);
    });
})();
