const fs = require('fs');
const path = require('path');
const { wooApi } = require('./woo-helpers');
const { logErrorToFile, logInfoToFile } = require('./logger');

const processMissingProducts = async (fileKey) => {
  const missingFilePath = path.join(__dirname, `missing_products_${fileKey}.json`);
  
  if (!fs.existsSync(missingFilePath)) {
    logInfoToFile(`No missing products file found for ${fileKey}`);
    return;
  }
  
  let missingProducts = [];
  try {
    missingProducts = JSON.parse(fs.readFileSync(missingFilePath, 'utf8'));
  } catch (err) {
    logErrorToFile(`Error reading missing products file: ${err.message}`);
    return;
  }
  
  for (const productData of missingProducts) {
    try {
      // Prepare the new product data – adjust fields as needed
      const newProduct = {
        name: productData.part_title || productData.part_number,
        sku: productData.sku || productData.part_number,
        description: productData.part_description || "",
        // Map additional fields and meta_data as needed:
        meta_data: [
          { key: "manufacturer", value: productData.manufacturer },
          // Include more meta data mappings here...
        ]
      };
      
      const response = await wooApi.post("products", newProduct);
      logInfoToFile(`Created new product for part_number=${productData.part_number} with ID ${response.data.id}`);
    } catch (error) {
      logErrorToFile(`Error creating product for part_number=${productData.part_number}: ${error.message}`);
    }
  }
};

// Get the fileKey from command line arguments, or default to a known value
const fileKey = process.argv[2] || "default";
processMissingProducts(fileKey);


// Example usage:
// node create-missing-products.js my_file_key
// node create-missing-products.js "20250218_Product-Update-Crystals.csv"