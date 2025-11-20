How the WooCommerce Product Updater Processes CSV Files and Updates Existing Products
⭐ Overview

The Product Updater reads product CSV files from AWS S3, processes each row, matches it to an existing WooCommerce product, and updates that product’s details (title, SKU, quantity, attributes, etc.).
It is designed to handle very large datasets (300,000+ rows) safely, with:

Concurrency controls

Automatic retry logic

Redis-based progress tracking

Checkpoints to resume after crashes

Separation of update + missing-product creation flows

This document explains the full update path, from S3 → worker → WooCommerce.

1. 📥 Fetching CSV Files from S3

File involved: s3-helpers.js

1.1. S3 file discovery

The updater connects to your S3 bucket (woocommerce-product-data-for-auto-update)

It lists all CSV files (or the specific one passed in fileKey)

1.2. Counting CSV rows

Before processing:

The script reads the CSV file once to count total rows

Saves total row count into Redis:

total-rows:<fileKey> = 10000


This ensures accurate progress percentages.

2. 📤 Submitting Jobs to the Batch Queue

Files:

index.js

queue.js

job-manager.js

For each chunk of rows in the CSV:

2.1. Chunking

The CSV streaming parser splits rows into batches (e.g., batchSize = 20)

Each batch becomes a BullMQ job

2.2. Jobs stored in Redis

Jobs include:

{
  batchId,
  fileKey,
  rows: [ { part_number, fields... }, ... ],
  attempt: 1
}

2.3. Concurrency control

Your settings (via .env):

BATCH_SIZE=20
CONCURRENCY=2


So worker will process max 2 batches at once.

3. ⚙️ Worker Starts Processing Rows

File: worker.js

Each batch of rows is passed into:

processBatch(job.data)


Inside this function:

For each CSV row:

Normalize fields (normalizeText())

Clean values like "N/A", "—", "-", ""

Determine product identity

Best match: part_number

Fallback: search by title or any custom identifiers

Retrieve WooCommerce product using:

getProductIdByPartNumber

getProductById

If product does not exist:

Add to missing_products_<fileKey>.json

Increment “skipped” counter

Continue to next row
This becomes part of the missing-product creation pipeline.

4. 🔄 Updating the WooCommerce Product

Files:

batch-helpers.js

woo-helpers.js

Once the worker identifies the existing WooCommerce product ID:

4.1. Build update payload

For example:

{
  name: "IC MCU 32BIT 128KB FLASH",
  short_description: "...",
  categories: [...],
  meta_data: [...],
  stock_quantity: 100,
  images: [...],
}


Validation happens here:

Strip HTML

Replace invalid characters

Normalize numbers

4.2. Send update

wooApi.put("products/<id>", payload)

4.3. Retry logic

If Woo API fails:

Timeouts

500 errors

WP Engine rate-throttle issues

429 back-pressure

499 disconnects

Then:

Retries job with exponential backoff

If final retry fails → recorded into failed-products_<fileKey>.json

5. 📉 Progress Tracking (Row Level)

Files:

checkpoint.js

process_checkpoint.json

For every successfully processed row:

rowLevel: {
  lastProcessedRow: 1920,
  totalRows: 3840,
  updated: 2908,
  skipped: 643,
  failed: 289,
  completedRows: 3840,
  remainingRows: 0
}


Tracking stored in both:

Redis (for real-time UI display)

process_checkpoint.json (for restart)

This ensures:

App can resume after crashes

Accurate progress => prevents 110%+ progress bug

6. 📦 Completion Logic

When all batches for a file are processed:

6.1. Mark file as completed

Redis keys updated:

fileStatus:<fileKey> = completed

6.2. Log results

update-progress.txt contains:

totalRows

updated

skipped

failed

time spent

etc.

6.3. Missing-product JSON written

Example filename:

missing_products_product-rf-transceiver-ics-02252025_part1.json


These will later be consumed by:
create-missing-products.js

7. ❌ What Happens to Errors?

Any errors in:

CSV parsing

WooCommerce update

Concurrency timeout

Redis issues

WP Engine throttling

are logged to:

logs/error-YYYY-MM-DD.log
failed-products_<fileKey>.json


Workers auto-retry where safe.

8. 🧩 After Updates Are Complete (Optional Follow-up)
8.1. Missing-product creation flow

Handled separately by:

node create-missing-products.js <fileKey>


This script:

Reads missing-products JSON

Resolves/creates categories

Creates WooCommerce product using cleaned vendor data

8.2. Frontend filters & ACF rebuilding

Since product data changes:

category filter caches may be refreshed

manufacturer/category map may be updated

wp87_product_search table may be rebuilt

But these are not part of the direct update flow.

🎉 Final Summary (Beginner-friendly)
When a CSV file arrives:

Updater reads it from S3

Splits rows into batches

Each batch becomes a queue job

Worker processes each row

Finds existing or missing products

Updates existing products via Woo API

Saves missing ones into JSON

Tracks every row in Redis + checkpoint file

Logs successes, failures, and summary

Finishes cleanly and waits for next file