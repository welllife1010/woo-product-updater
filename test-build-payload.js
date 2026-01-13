const { buildUpdatePayload, isProtectedImageUrl } = require('./src/batch/build-update-payload');

// Test 1: Image URL protection
console.log('=== Test 1: Image URL Protection ===');
const stagingUrl = 'https://env-suntsucom-staging.kinsta.cloud/wp-content/uploads/2020/08/SXT224.png';
const productionUrl = 'https://suntsu.com/wp-content/uploads/2020/08/SXT224.png';

const result1 = isProtectedImageUrl(stagingUrl, productionUrl);
console.log('Staging -> Production:', result1);

const result2 = isProtectedImageUrl(productionUrl, stagingUrl);
console.log('Production -> Staging:', result2);

const result3 = isProtectedImageUrl(stagingUrl, '');
console.log('Staging -> Empty:', result3);

// Test 2: Full payload build (only changed fields)
console.log('');
console.log('=== Test 2: Diff-Based Payload ===');
const currentData = {
  meta_data: [
    { key: 'image_url', value: stagingUrl },
    { key: 'quantity', value: '100' },
    { key: 'htsus_code', value: '8541.10.0000' },
  ]
};

const candidateData = {
  id: 12345,
  meta_data: [
    { key: 'image_url', value: productionUrl },  // Should be SKIPPED (cross-env)
    { key: 'quantity', value: '100' },           // Should be SKIPPED (same)
    { key: 'htsus_code', value: '8542.39.0001' }, // Should be UPDATED (different)
  ]
};

const { payload, changedFields, skippedFields } = buildUpdatePayload(
  currentData, candidateData, 'TEST-123', 'test.csv'
);

console.log('Changed fields:', changedFields);
console.log('Skipped fields:', Object.keys(skippedFields));
console.log('Skipped reasons:', skippedFields);
console.log('Payload:', JSON.stringify(payload, null, 2));

process.exit(0);
