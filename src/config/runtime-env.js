'use strict';

/**
 * Central runtime environment resolution.
 *
 * Option A: prefer a single, unambiguous selector: APP_ENV
 *   - production | staging | development
 *
 * Back-compat during migration:
 *   - If APP_ENV is missing, fall back to EXECUTION_MODE
 *   - Translate legacy values:
 *       test/stage -> staging
 *
 * This module is intentionally dependency-free (no logger, no dotenv) so it can
 * be used from anywhere without creating circular deps.
 */

const ALLOWED_APP_ENVS = new Set(['production', 'staging', 'development']);

function normalizeEnvValue(v) {
  return String(v || '').trim().toLowerCase();
}

function resolveAppEnv(env = process.env) {
  const raw = normalizeEnvValue(env.APP_ENV || env.EXECUTION_MODE || 'production');

  // Legacy translations
  const translated =
    raw === 'test' || raw === 'stage' || raw === 'stg' ? 'staging' :
    raw === 'prod' ? 'production' :
    raw;

  if (!ALLOWED_APP_ENVS.has(translated)) {
    throw new Error(
      `Invalid APP_ENV/EXECUTION_MODE "${raw}". ` +
        `Use one of: production, staging, development.`
    );
  }

  return translated;
}

function getEnvLabel(appEnv) {
  switch (appEnv) {
    case 'production':
      return 'PROD';
    case 'staging':
      return 'STAGING';
    case 'development':
      return 'DEV';
    default:
      return String(appEnv || '').toUpperCase() || 'UNKNOWN';
  }
}

function firstNonEmpty(...values) {
  for (const v of values) {
    if (v !== undefined && v !== null && String(v).trim() !== '') return v;
  }
  return undefined;
}

function getWooConfig(env = process.env, appEnv = resolveAppEnv(env)) {
  // New, explicit names
  const production = {
    url: firstNonEmpty(env.WOO_API_BASE_URL_PRODUCTION, env.WOO_API_BASE_URL),
    consumerKey: firstNonEmpty(env.WOO_API_CONSUMER_KEY_PRODUCTION, env.WOO_API_CONSUMER_KEY),
    consumerSecret: firstNonEmpty(
      env.WOO_API_CONSUMER_SECRET_PRODUCTION,
      env.WOO_API_CONSUMER_SECRET
    ),
  };

  const staging = {
    url: firstNonEmpty(env.WOO_API_BASE_URL_STAGING, env.WOO_API_BASE_URL_TEST),
    consumerKey: firstNonEmpty(
      env.WOO_API_CONSUMER_KEY_STAGING,
      env.WOO_API_CONSUMER_KEY_TEST
    ),
    consumerSecret: firstNonEmpty(
      env.WOO_API_CONSUMER_SECRET_STAGING,
      env.WOO_API_CONSUMER_SECRET_TEST
    ),
  };

  const development = {
    url: firstNonEmpty(env.WOO_API_BASE_URL_DEVELOPMENT, env.WOO_API_BASE_URL_DEV),
    consumerKey: firstNonEmpty(
      env.WOO_API_CONSUMER_KEY_DEVELOPMENT,
      env.WOO_API_CONSUMER_KEY_DEV
    ),
    consumerSecret: firstNonEmpty(
      env.WOO_API_CONSUMER_SECRET_DEVELOPMENT,
      env.WOO_API_CONSUMER_SECRET_DEV
    ),
  };

  if (appEnv === 'staging') return staging;
  if (appEnv === 'development') return development;
  return production;
}

function getS3BucketName(env = process.env, appEnv = resolveAppEnv(env)) {
  const production = firstNonEmpty(env.S3_BUCKET_NAME_PRODUCTION, env.S3_BUCKET_NAME);
  const staging = firstNonEmpty(env.S3_BUCKET_NAME_STAGING, env.S3_BUCKET_NAME_TEST);
  const development = firstNonEmpty(
    env.S3_BUCKET_NAME_DEVELOPMENT,
    // Legacy behavior had dev share TEST bucket
    env.S3_BUCKET_NAME_TEST
  );

  if (appEnv === 'staging') return staging;
  if (appEnv === 'development') return development;
  return production;
}

function requireNonEmpty(value, name) {
  if (value === undefined || value === null || String(value).trim() === '') {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

module.exports = {
  ALLOWED_APP_ENVS,
  resolveAppEnv,
  getEnvLabel,
  getWooConfig,
  getS3BucketName,
  requireNonEmpty,
};
