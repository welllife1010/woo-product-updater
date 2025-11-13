// __tests__/map-new-data.test.js
const { createNewData, normalizeCsvHeaders } = require("../src/batch/map-new-data");

// Helper: turn meta_data array -> object { key: value }
const metaToObject = (metaArray) =>
  metaArray.reduce((acc, { key, value }) => {
    acc[key] = value;
    return acc;
  }, {});

describe("map-new-data.js - createNewData", () => {
  test("maps new template fields into correct ACF meta keys", () => {
    // Simulate a row as parsed by csv-parser (raw header names)
    const item = {
      "Manufacturer Part Number": "ABC123",
      "Product Description": "Small widget",
      "Packaging": "Tape & Reel",
      "Stock Quantity": "2500",
      "Voltage": "5V",
      "Operating Temperature": "-40°C to 85°C",
      "Supplier Device Package": "SOT-23",
      "RoHS Compliance": "Compliant",
      "REACH Compliance": "Compliant",
      "HTS Code": "8536.90.4000",
      "ECCN": "EAR99",
      "Moisture Sensitivity Level": "3",
      "Tags": "rf|mcus"
    };

    const productId = 123;

    // Ensure UPDATE_MODE is full for this test
    process.env.UPDATE_MODE = "full";

    const payload = createNewData(item, productId, "ABC123");
    const meta = metaToObject(payload.meta_data);

    // Description precedence: detail_description > short_description > part_description
    // Here we only provided Product Description -> part_description -> detail_description
    expect(payload.description).toBe("Small widget");

    // Core identification fields
    expect(payload.id).toBe(productId);
    expect(payload.part_number).toBe("ABC123");
    expect(payload.sku).toContain("ABC123"); // basic sanity check

    // Check meta mappings
    expect(meta.packaging).toBe("Tape & Reel");
    // quantity (from Stock Quantity -> quantity alias -> quantity meta)
    expect(meta.quantity).toBe("2500");
    expect(meta.voltage).toBe("5V");
    expect(meta.operating_temperature).toBe("-40°C to 85°C");
    expect(meta.supplier_device_package).toBe("SOT-23");
    expect(meta.rohs_status).toBe("Compliant");
    expect(meta.reach_status).toBe("Compliant");
    expect(meta.htsus_code).toBe("8536.90.4000");
    expect(meta.export_control_class_number).toBe("EAR99");
    expect(meta.moisture_sensitivity_level).toBe("3");

    // additional_key_information exists (even if we don't assert content yet)
    expect(meta.additional_key_information).toBeDefined();
  });

  test("UPDATE_MODE=quantity uses row.quantity (stock_quantity / quantity_available)", () => {
    const item = {
      "Manufacturer Part Number": "ABC123",
      "Stock Quantity": "999"
    };

    const productId = 456;
    process.env.UPDATE_MODE = "quantity"; // quantity-only mode

    const payload = createNewData(item, productId, "ABC123");
    const meta = metaToObject(payload.meta_data);

    // Should only have quantity meta (for this simple test)
    expect(meta.quantity).toBe("999");
    expect(payload.part_number).toBe("ABC123");
  });
});
