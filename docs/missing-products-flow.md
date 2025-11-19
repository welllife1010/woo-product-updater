Missing Products Flow (from CSV → JSON → New Woo Products)

This section explains how the system handles “missing products” – rows in a CSV that didn’t match any existing WooCommerce product during the normal update process – and how we later use those rows to create brand-new Woo products.

This is written for future me / non-experts, so it avoids scary jargon as much as possible.

1. What is a “missing product”?

During the main batch update (the normal CSV → Woo update pipeline), each CSV row tries to find a matching WooCommerce product (by part number, SKU, etc.).

If a matching Woo product is found → we update it.

If no product is found → we treat that CSV row as a “missing product”.

Instead of losing that row, we save it to a JSON file on disk so that we can later review it and optionally create a brand-new product in WooCommerce.

This capture is handled by:

src/batch/io-status.js → recordMissingProduct(fileKey, item)

2. Where do missing products get saved?

Missing products are grouped by:

Leaf category of the row (most specific category, e.g. Microcontrollers), and

CSV file they came from (via fileKey).

2.1. Leaf category → folder name

From the CSV row, we look at:

item.category or item.Category

Example value:

"Integrated Circuits (ICs)>Embedded>Microcontrollers"


We:

Split on > → ["Integrated Circuits (ICs)", "Embedded", "Microcontrollers"]

Take the last part → "Microcontrollers" (this is the leaf category)

Turn it into a slug → "microcontrollers"

That gives us a folder name:

missing-microcontrollers

2.2. fileKey → cleanFileKey

fileKey is the original CSV identifier (often the S3 key or local filename), for example:

product-microcontrollers-03112025_part4.csv

LED-Emitters-IR-UV-Visible.csv

vendor-x/ics/microcontrollers-part2.csv

We drop the .csv (and any folder prefix when needed) and use the base name as cleanFileKey, e.g.

"product-microcontrollers-03112025_part4.csv"
  → "product-microcontrollers-03112025_part4"

2.3. Full path layout

Putting the pieces together, each “missing products” JSON is saved as:

./missing-products/
  missing-[leafCategorySlug]/
    missing_products_[cleanFileKey].json


Examples

CSV: product-microcontrollers-03112025_part4.csv
Leaf category: "Microcontrollers" → slug "microcontrollers"

→ file:

./missing-products/missing-microcontrollers/
  missing_products_product-microcontrollers-03112025_part4.json


CSV: LED-Emitters-IR-UV-Visible.csv
Leaf category: "LED Emitters - IR, UV, Visible" → slug "led-emitters-ir-uv-visible"

→ file:

./missing-products/missing-led-emitters-ir-uv-visible/
  missing_products_LED-Emitters-IR-UV-Visible.json


All of this grouping / writing is done by:

src/batch/io-status.js → recordMissingProduct(fileKey, item)

3. How are new Woo products created from these JSON files?

Later, when we decide we want to actually create products for the missing entries, we run a separate script:

create-missing-products.js → processMissingProducts(categorySlug, fileKey)


This script:

Reads the appropriate missing-products JSON file.

For each row:

Resolves a category hierarchy from the row’s category field
(fuzzy match via category-map.js + fallback > split).

Ensures those categories exist in Woo:

Creates main / sub / leaf categories if needed.

Builds a WooCommerce “create product” payload:

name, sku, description

categories (by ID)

meta_data (manufacturer, series, etc.)

Calls POST /wp-json/wc/v3/products via the Woo API.

Logs success/failure to output-files/info-log.txt and output-files/error-log.txt.

3.1. Where does it read from?

processMissingProducts(categorySlug, fileKey) reconstructs the path using:

categorySlug → the leaf category slug, e.g. "microcontrollers".

fileKey → the original CSV key or filename, e.g. "product-microcontrollers-03112025_part4.csv".

It then builds:

cleanFileKey = getCleanFileKey(fileKey)
// e.g. "product-microcontrollers-03112025_part4.csv"
//   → "product-microcontrollers-03112025_part4"

missingFilePath =
  ./missing-products/missing-[categorySlug]/missing_products_[cleanFileKey].json


So for:

categorySlug = "microcontrollers"
fileKey      = "product-microcontrollers-03112025_part4.csv"


it will read:

./missing-products/missing-microcontrollers/
  missing_products_product-microcontrollers-03112025_part4.json

4. How to run the “create missing products” script (CLI)

From the project root:

# Recommended: development / test mode first
EXECUTION_MODE=development NODE_ENV=test \
  node create-missing-products.js <categorySlug> <fileKey>


Arguments:

<categorySlug>
The leaf category slug used in the folder name:

microcontrollers

led-emitters-ir-uv-visible

resistors

<fileKey>
The CSV key or filename used during the original batch:

product-microcontrollers-03112025_part4.csv

LED-Emitters-IR-UV-Visible.csv

Example:

EXECUTION_MODE=development NODE_ENV=test \
  node create-missing-products.js microcontrollers product-microcontrollers-03112025_part4.csv


This will:

Look for:
./missing-products/missing-microcontrollers/missing_products_product-microcontrollers-03112025_part4.json

Process each missing row:

Resolve / create categories.

Create new Woo products.

You can then confirm in Woo dev:

Products → All Products → search by part_number (SKU).

Products → Categories → confirm the category hierarchy was created.

5. Where to see what happened (logs)

The script logs to files under ./output-files/:

info-log.txt
High-level info, e.g.:

[create-missing-products] 🚀 Processing 10 missing products from ...
[create-missing-products] ✅ Loaded 61 existing product categories from WooCommerce
[create-missing-products] Category for part_number=TEST-CAT-002: "ICs>Embedded>Microcontrollers" → ...
[create-missing-products] ✅ Created product part_number=TEST-CAT-002 (id=348657) with categories: [...]


error-log.txt
Errors from Woo or internal logic, e.g.:

[create-missing-products] ❌ Error creating product for part_number=TEST-CAT-001:
  status=400, message=Invalid or duplicated SKU., ...


Always check error-log.txt if the script finishes “too fast” or you don’t see new products.

6. Quick mental model (for future Silvia)

During update, anything that doesn’t match an existing Woo product is:

captured by recordMissingProduct(fileKey, item)

and written to a JSON file grouped by leaf category + source CSV.

Later, when you’re ready to insert those missing products:

you run create-missing-products.js <categorySlug> <fileKey>

the script reads that JSON, resolves categories, and creates actual products in Woo.

If, months from now, you forget what categorySlug vs fileKey mean:

categorySlug → “Which category bucket?” (leaf category, like microcontrollers)

fileKey → “Which CSV batch?” (source file name, like product-microcontrollers-03112025_part4.csv)