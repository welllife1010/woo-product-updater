/**
 * Central place for CSV-related environment configuration.
 */

const CSV_HEADER_ROW = Number(process.env.CSV_HEADER_ROW || "10");
const CSV_SKIP_LINES = CSV_HEADER_ROW > 0 ? CSV_HEADER_ROW - 1 : 0;

module.exports = {
  CSV_HEADER_ROW,
  CSV_SKIP_LINES,
};
