function isNonEmptyString(v) {
  return typeof v === "string" && v.trim().length > 0;
}

function isMappingComplete(mapping) {
  if (!mapping || typeof mapping !== "object") return false;
  return (
    isNonEmptyString(mapping.partNumber) &&
    isNonEmptyString(mapping.manufacturer) &&
    isNonEmptyString(mapping.category)
  );
}

function getReadyFiles(mappings) {
  const files = Array.isArray(mappings?.files) ? mappings.files : [];
  return files.filter((f) => f && f.status === "ready");
}

function getStartCandidateFiles(mappings) {
  const files = Array.isArray(mappings?.files) ? mappings.files : [];

  // Treat a mapped PENDING file as start-eligible (we will auto-promote it to READY on start).
  // This fixes the common “I edited csv-mappings.json manually but Start is still disabled” issue.
  return files.filter((f) => {
    if (!f) return false;
    if (f.status === "ready") return true;
    if (f.status === "pending" && isMappingComplete(f.mapping)) return true;
    return false;
  });
}

function validateCanStartFromMappings(mappings) {
  const startCandidates = getStartCandidateFiles(mappings);

  const reasons = [];
  if (!startCandidates.length) {
    reasons.push(
      "No CSV files are eligible to run (mark as READY or provide a complete mapping)"
    );
  }

  const missingMapping = startCandidates
    .filter((f) => f.status === "ready" && !isMappingComplete(f.mapping))
    .map((f) => f.fileKey);

  if (missingMapping.length) {
    reasons.push(
      `Missing required mapping (part number, manufacturer, category) for: ${missingMapping.join(
        ", "
      )}`
    );
  }

  return {
    ok: reasons.length === 0,
    reasons,
    startCandidates,
  };
}

function isRunActiveFromProgress(progressByFileKey) {
  if (!progressByFileKey || typeof progressByFileKey !== "object") return false;

  // Consider a run active if any file has totalRows > 0 and completed < totalRows.
  for (const stats of Object.values(progressByFileKey)) {
    if (!stats) continue;
    const total = Number(stats.totalRows) || 0;
    const completed = Number(stats.completed) || 0;
    if (total > 0 && completed < total) return true;
  }

  return false;
}

module.exports = {
  isMappingComplete,
  getReadyFiles,
  getStartCandidateFiles,
  validateCanStartFromMappings,
  isRunActiveFromProgress,
};
