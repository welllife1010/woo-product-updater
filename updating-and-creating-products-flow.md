# Updating and Creating Products – Flow Guide

This document explains, step by step, how product data flows through the system for:

1. Updating existing products  
2. Creating missing products later

---

# 1. Product Update Flow (Existing Products)

## Step 1 – Start the update process

Run:

```bash
node src/batch/index.js
# or
node worker.js
```

## Step 2 – For each row: find existing product

`fetchProductData(item, index, total, fileKey)`:

1. `normalizeManufacturerName()` (optional but recommended)
2. `getProductIdByPartNumber(part_number, manufacturer)`
3. If product exists → fetch it via `getProductById`

## Step 3 – If product is missing

1. Extract category text (`item.category`)
2. `leafSlug = resolveLeafSlugSmart(rawCategory)`
3. Save row via:

```txt
./missing-products/
  missing-[leafSlug]/
    missing_products_[cleanFileKey].json
```

## Step 4 – If product is found

1. `validateProductMatch`
2. Map CSV to update payload
3. Woo batch update request
4. Log updated/skipped/failed

## Step 5 – Save batch status & checkpoints

Saved under:

```txt
batch_status/<fileKey>/batch_status.json
```

---

# 2. Missing Product Creation Flow

Use:

```bash
node create-missing-products.js <categorySlug> <fileKey>
```

Example:

```bash
node create-missing-products.js microcontrollers product-microcontrollers-03112025_part4.csv
```

## What it does:

1. Reads:

```txt
missing-products/missing-[categorySlug]/missing_products_[cleanFileKey].json
```

2. For each row:

- `resolveCategorySmart(rawCategory)`
- `ensureCategoryHierarchyIds`
- Build Woo "create product" payload
- `wooApi.post("products", payload)` (if EXECUTION_MODE=production)

3. Manufacturer normalization recommended for consistent identity.

---

# 3. Summary

- Update script handles existing products & records missing ones.
- Missing-product files store unmatched rows grouped by leaf slug + file.
- Creation script converts those rows into real Woo products.
- Category resolver + manufacturer normalization ensure consistency.
