# WooCommerce Product Updater

This Node.js project keeps a large WooCommerce catalog in sync with vendor CSV feeds.

It has **two main jobs**:

1. **Update existing products** when we receive fresh CSV data.
2. **Record “missing” products** (rows that don’t match anything in Woo), and later
   **create those missing products** in Woo using a separate script.

The system uses:

- Node.js (batch scripts, workers)
- WooCommerce REST API
- BullMQ + Redis (for queuing batch jobs)
- AWS S3 (for vendor CSV files)
- Local JSON files (for logs, checkpoints, and missing-product capture)
- Fuzzy matching (Fuse.js) for category + manufacturer normalization

---

## High-Level Architecture

┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  index.js       │     │  worker.js      │     │ csv-mapping-    │
│  (Main App)     │────▶│  (Worker)       │     │ server.js (UI)  │
│  Port 3000      │     │                 │     │  Port 4000      │
│                 │     │                 │     │                 │
│ - Reads CSV     │     │ - Processes     │     │ - Admin UI      │
│ - Creates jobs  │     │   batch jobs    │     │ - Upload CSVs   │
│ - Enqueues to   │     │ - Updates Woo   │     │ - Map columns   │
│   Redis         │     │ - Tracks        │     │ - Monitor       │
└─────────────────┘     │   progress      │     │   progress      │
        │               └─────────────────┘     └─────────────────┘
        │                       │                        │
        └───────────────────────┼────────────────────────┘
                                ▼
                   ┌─────────────────────┐
                   │       Redis         │
                   │   (Job Queue +      │
                   │    Progress Data)   │
                   └─────────────────────┘

### Data Sources

- **CSV files** from vendors (usually uploaded to S3)
- **WooCommerce REST API**:
  - `products`
  - `products/categories`

### Core Concepts

- **Part identity**:  
  Products are identified by:

  - `part_number` (should be unique & consistent)
  - `manufacturer` (normalized via a mapping, e.g. `"NXP Semiconductors"` → `"NXP"`)

- **Update vs. Create**:
  - **Update flow**:
    - If `(part_number, manufacturer)` matches a WooCommerce product, we update it.
  - **Missing-products flow**:
    - If we **cannot find** a matching product, we record the full CSV row into a JSON file under `./missing-products/`.
    - Later, we run `create-missing-products.js` to turn those JSON rows into *new* Woo products.

- **Category resolution**:
  - We use `category-resolver.js` to map vendor category text like
    `"Integrated Circuits (ICs)>Embedded>Microcontrollers"` to our Woo categories.
  - We first try **existing Woo categories** (via Woo API + Fuse.js).
  - If that fails, we fall back to CSV / reference mapping (`category-map.js`).
  - This is used both for:
    - deciding where to group missing-products JSON files (by leaf slug), and
    - deciding which categories to create/attach when creating new products.

---

## Folder Structure (simplified)

```txt
project-root/
  .env
  ecosystem.config.js
  package.json

  category-resolver.js
  category-woo.js
  create-missing-products.js

  src/
    batch/
      index.js
      queue.js
      job-manager.js
      fetch-validate.js
      io-status.js
      map-new-data.js
      utils.js
```

---

## Key Scripts

### 1. Product Update Script

```bash
node src/batch/index.js
# or
node worker.js
```

### 2. Missing Product Creation Script

```bash
node create-missing-products.js <categorySlug> <fileKey>
```

Example:

```bash
node create-missing-products.js microcontrollers product-microcontrollers-03112025_part4.csv
```

---

## Category Resolution Summary

- `resolveCategoryFromWooFuzzy(rawCategory)` – fuzzy match against existing Woo categories.
- `resolveCategorySmart(rawCategory)` – try Woo fuzzy first, then CSV mapping.
- `resolveLeafSlugSmart(rawCategory)` – returns Woo leaf slug for storing missing products.

---

## Missing Products Structure

```txt
./missing-products/
  missing-[leafCategorySlug]/
    missing_products_[cleanFileKey].json
```

---

## Manufacturer Normalization

Use a helper such as:

```js
normalizeManufacturerName("NXP Semiconductors") → "NXP"
```

Apply it:

- Before identity matching (`getProductIdByPartNumber`)
- When creating new products (store canonical manufacturer)

---

## Typical Workflow

1. Upload vendor CSVs
2. Run update
3. Review missing-products
4. Run creation script per category/file
