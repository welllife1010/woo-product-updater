const fs = require("fs");

function ensureMappingsFile(mappingsPath) {
  if (!fs.existsSync(mappingsPath)) {
    fs.writeFileSync(mappingsPath, JSON.stringify({ files: [] }, null, 2));
  }
}

function readMappings(mappingsPath) {
  ensureMappingsFile(mappingsPath);

  const data = JSON.parse(fs.readFileSync(mappingsPath, "utf8"));
  if (Array.isArray(data)) return { files: data };
  if (data && Array.isArray(data.files)) return data;
  return { files: [] };
}

function writeMappings(mappingsPath, data) {
  fs.writeFileSync(mappingsPath, JSON.stringify(data, null, 2));
}

module.exports = {
  readMappings,
  writeMappings,
};
