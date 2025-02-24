const createUniqueJobId = (fileKey, action = "", rowIndex = "", retryCount = "") => {
    // ✅ Ensure rowIndex and retryCount are valid integers or default to 0
    const validRowIndex = Number.isInteger(Number(rowIndex)) ? Number(rowIndex) : 0;
    const validRetryCount = Number.isInteger(Number(retryCount)) ? Number(retryCount) : 0;

    // ✅ Ensure fileKey and action are valid strings
    const validFileKey = typeof fileKey === "string" ? fileKey.replace(/\s+/g, "_") : "unknown-file";
    const validAction = typeof action === "string" && action ? `_${action.replace(/\s+/g, "_")}` : "";

    // ✅ Generate timestamp for uniqueness
    const timestamp = Date.now();

    // ✅ Construct Job ID safely
    let jobId = `jobId_${validFileKey}${validAction}_row-${validRowIndex}_retry-${validRetryCount}_${timestamp}`;

    return jobId;
};

module.exports = { createUniqueJobId };
