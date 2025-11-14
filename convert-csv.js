const fs = require('fs');
const path = require('path');
const { parse } = require('json2csv');
require('dotenv').config();

const invalidFilePath = path.join(__dirname, '/input-json/invalid_datasheet_urls_controllers.json');
const failedFilePath = path.join(__dirname, '/input-json/failed.json');
const outputCsvPath = path.join(__dirname, '/output-csv/filtered_datasheet_urls.csv');

try {
    // Read and parse both JSON files
    const invalidData = JSON.parse(fs.readFileSync(invalidFilePath, 'utf-8'));
    const failedData = JSON.parse(fs.readFileSync(failedFilePath, 'utf-8'));

    // Create a set of IDs from the failed data for quick lookup
    const failedIds = new Set(failedData.map(item => item.id));

    // Filter out entries from invalidData whose IDs are in failedIds
    const filteredData = invalidData.filter(item => !failedIds.has(item.id));

    // Prepare data for CSV output with custom Datasheet_URL
    const csvData = filteredData.map(item => {
        const categorySlug = item.category; // Use category directly as the slug
        let partNumber = item.title.replace(/\//g, '-'); // Replace slashes with dashes
        const datasheetUrl = `${process.env.S3_Products_Base_URL}${categorySlug}/${partNumber}.pdf`;

        return {
            Part_Id: item.id,
            Part_Number: partNumber,
            Datasheet_URL: datasheetUrl
        };
    });

    // Convert JSON to CSV
    const csv = parse(csvData, { fields: ['Part_Id', 'Part_Number', 'Datasheet_URL'] });

    // Write the CSV to a file
    fs.writeFileSync(outputCsvPath, csv, 'utf-8');
    console.log(`Filtered data successfully written to ${outputCsvPath}`);
} catch (error) {
    console.error('Error processing files:', error.message);
}