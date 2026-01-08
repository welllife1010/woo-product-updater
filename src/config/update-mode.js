/**
 * Runtime update mode configuration.
 * 
 * This module manages the UPDATE_MODE setting which can be changed at runtime
 * without restarting the worker processes.
 * 
 * UPDATE_MODE values:
 *   - "full": Update all product fields (default)
 *   - "quantity": Update only quantity field (faster)
 * 
 * The mode is stored in a JSON file that workers read on each batch,
 * allowing the UI to change modes without process restarts.
 */

const fs = require('fs');
const path = require('path');

// Config file location - use process.cwd() for server compatibility
// Falls back to path relative to this file for local dev
function getConfigFilePath() {
  // Try process.cwd() first (where PM2 runs from)
  const cwdPath = path.join(process.cwd(), 'update-mode.json');
  // Fallback to relative path from this file
  const relativePath = path.join(__dirname, '../../update-mode.json');
  
  // If file exists in cwd, use that; otherwise use relative path
  if (fs.existsSync(cwdPath)) {
    return cwdPath;
  }
  return relativePath;
}

/**
 * Get the current update mode.
 * Reads from config file first, falls back to env var, then defaults to "full".
 * @returns {"full" | "quantity"}
 */
function getUpdateMode() {
  const configFile = getConfigFilePath();
  try {
    if (fs.existsSync(configFile)) {
      const data = JSON.parse(fs.readFileSync(configFile, 'utf8'));
      if (data.mode === 'quantity' || data.mode === 'full') {
        console.log(`[UPDATE_MODE] Read from config: ${data.mode} (file: ${configFile})`);
        return data.mode;
      }
    }
  } catch (err) {
    console.error(`[UPDATE_MODE] Error reading config file: ${err.message}`);
  }
  
  // Fall back to environment variable
  const envMode = (process.env.UPDATE_MODE || '').toLowerCase();
  if (envMode === 'quantity') {
    console.log(`[UPDATE_MODE] Using env var: quantity`);
    return 'quantity';
  }
  
  console.log(`[UPDATE_MODE] Using default: full`);
  return 'full';
}

/**
 * Set the update mode.
 * Writes to config file so workers pick it up on next batch.
 * @param {"full" | "quantity"} mode
 */
function setUpdateMode(mode) {
  const validMode = mode === 'quantity' ? 'quantity' : 'full';
  const data = {
    mode: validMode,
    updatedAt: new Date().toISOString(),
  };
  
  // Write to both possible locations for reliability
  const cwdPath = path.join(process.cwd(), 'update-mode.json');
  const relativePath = path.join(__dirname, '../../update-mode.json');
  
  try {
    fs.writeFileSync(cwdPath, JSON.stringify(data, null, 2));
    console.log(`[UPDATE_MODE] Wrote to: ${cwdPath}`);
  } catch (err) {
    console.error(`[UPDATE_MODE] Failed to write to ${cwdPath}: ${err.message}`);
  }
  
  try {
    fs.writeFileSync(relativePath, JSON.stringify(data, null, 2));
    console.log(`[UPDATE_MODE] Wrote to: ${relativePath}`);
  } catch (err) {
    console.error(`[UPDATE_MODE] Failed to write to ${relativePath}: ${err.message}`);
  }
  
  return validMode;
}

/**
 * Reset to default mode (full).
 */
function resetUpdateMode() {
  return setUpdateMode('full');
}

module.exports = {
  getUpdateMode,
  setUpdateMode,
  resetUpdateMode,
};
