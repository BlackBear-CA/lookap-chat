const { BlobServiceClient } = require('@azure/storage-blob');
const OpenAI = require('openai');
const csv = require('fast-csv');

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const DATASETS_CONTAINER = "datasets";

module.exports = async function (context, req) {
    context.log("🔹 Chat function triggered.");

    const userMessage = req.body && req.body.userMessage ? req.body.userMessage.trim() : null;
    context.log("📩 Received user message:", userMessage);

    if (!userMessage) {
        context.log("⚠️ No userMessage found in request body.");
        context.res = { status: 400, body: { message: "Error: No userMessage found in request body." } };
        return;
    }

    try {
        // 🔎 Step 1: Use OpenAI to analyze user intent
        const { dataset, column, value } = await analyzeUserQuery(userMessage);
        if (dataset && column && value) {
            context.log(`🔎 Identified dataset: ${dataset}, column: ${column}, value: ${value}`);
            
            try {
                // 🔎 Step 2: Query the dataset
                let searchResults = await searchDataset(context, dataset, column, value);
                context.log("📄 Search Results:", JSON.stringify(searchResults));

                if (searchResults.length > 0) {
                    context.res = { status: 200, body: { message: formatResults(searchResults) } };
                    return;
                } else {
                    context.log(`⚠️ No records found for '${value}' in ${dataset}.`);
                    context.res = { status: 200, body: { message: `No records found for '${value}' in ${dataset}.` } };
                    return;
                }
            } catch (error) {
                context.log("❌ ERROR: searchDataset() failed:", error.message);
                context.res = { status: 500, body: { message: "Error searching dataset: " + error.message } };
                return;
            }
        }

        // 🔎 Step 3: Fallback to OpenAI response if no dataset match
        context.log("💡 Sending user message to OpenAI API...");
        const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
        const chatResponse = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [{ role: "user", content: userMessage }]
        });

        const aiResponse = chatResponse.choices[0].message.content;
        context.log("🤖 OpenAI Response:", aiResponse);

        context.res = { status: 200, body: { message: aiResponse } };

    } catch (error) {
        context.log("❌ Error occurred:", error.message);
        context.res = { status: 500, body: { message: "Error processing request: " + error.message } };
    }
};

/**
 * 🔍 Uses OpenAI to analyze user queries and determine dataset, column, and search value.
 */
async function analyzeUserQuery(userMessage) {
    const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

    const prompt = `
    Given the following datasets, determine which dataset should be queried, 
    what column should be searched, and what value should be looked up.

    Available datasets:
    - "barcodes.csv" (Generic barcode for SKUs)
    - "materialBasicData.csv" (Basic SKU details: description, manufacturer, part numbers)
    - "purchaseRecords.csv" (Active purchase records: purchase order, vendor, date, delivery info)
    - "warehouseData.csv" (Stock levels, storage bins, UOM)
    - "stockTransactions.csv" (Material movements: purchases, goods issue, transfers)
    - "stockPricingData.csv" (Moving average price of SKU)
    - "reservationData.csv" (Internal reservations for SKU)

    User Query: "${userMessage}"

    Expected Output:
    Return JSON in this format:
    {"dataset": "purchaseRecords.csv", "column": "sku_id", "value": "10271"}

    If no dataset match is found, return {"dataset": null, "column": null, "value": null}.
    `;

    const response = await openai.chat.completions.create({
        model: "gpt-4",
        messages: [{ role: "system", content: prompt }]
    });

    try {
        return JSON.parse(response.choices[0].message.content);
    } catch {
        return { dataset: null, column: null, value: null };
    }
}

/**
 * 📂 Queries the identified dataset for a matching record.
 */
async function searchDataset(context, filename, column, value) {
    try {
        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const containerClient = blobServiceClient.getContainerClient(DATASETS_CONTAINER);
        const blobClient = containerClient.getBlobClient(filename);

        context.log(`📂 Checking existence of ${filename} in Blob Storage...`);
        const exists = await blobClient.exists();
        if (!exists) {
            throw new Error(`File ${filename} not found.`);
        }

        context.log(`⬇️ Downloading ${filename} from Blob Storage...`);
        const downloadResponse = await blobClient.download();
        const downloadedData = await streamToString(downloadResponse.readableStreamBody);

        if (!downloadedData || downloadedData.trim() === "") {
            throw new Error(`File ${filename} is empty or unreadable.`);
        }

        context.log(`📄 Parsing ${filename}...`);

        return new Promise((resolve, reject) => {
            let results = [];
            csv.parseString(downloadedData, { headers: true, trim: true })
                .on("data", (row) => {
                    if (row[column] && row[column].toLowerCase().includes(value.toLowerCase())) {
                        results.push(row);
                    }
                })
                .on("end", () => {
                    context.log(`✅ Found ${results.length} matching records in ${filename}`);
                    resolve(results);
                })
                .on("error", (err) => {
                    context.log(`❌ CSV Parsing Failed: ${err.message}`);
                    reject(err);
                });
        });
    } catch (error) {
        context.log("❌ Error processing dataset:", error.message);
        throw new Error(`Error processing dataset ${filename}: ${error.message}`);
    }
}

/**
 * 📜 Converts the results into a readable response format.
 */
function formatResults(results) {
    return results.map(row => 
        Object.entries(row).map(([key, value]) => `**${key}**: ${value}`).join("\n")
    ).join("\n\n");
}

/**
 * 📥 Converts a readable stream into a string.
 */
async function streamToString(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("end", () => resolve(Buffer.concat(chunks).toString()));
        stream.on("error", reject);
    });
}
