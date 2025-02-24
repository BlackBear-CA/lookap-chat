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
        context.res = { status: 400, body: { message: "Error: No userMessage found in request body." } };
        return;
    }

    try {
        // 🔎 Step 1: Identify the dataset, column, and value using OpenAI
        const { dataset, column, value } = await analyzeUserQuery(userMessage);
        
        if (dataset && column && value) {
            context.log(`📂 Attempting to fetch data from: ${dataset}, Column: ${column}, Value: ${value}`);

            try {
                // 🔎 Step 2: Query the dataset
                let searchResults = await searchDataset(context, dataset, column, value);

                if (searchResults.length > 0) {
                    context.res = { status: 200, body: { message: formatResults(searchResults) } };
                    return;
                } else {
                    context.res = { status: 200, body: { message: `No records found for '${value}' in ${dataset}.` } };
                    return;
                }
            } catch (error) {
                context.log("❌ ERROR: searchDataset() failed:", error.message);
                context.res = { status: 500, body: { message: "Error searching dataset: " + error.message } };
                return;
            }
        }

        // 🔎 Step 3: If no structured query match, fallback to OpenAI chat response
        context.log("💡 Sending user message to OpenAI API...");
        const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
        const chatResponse = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [{ role: "user", content: userMessage }]
        });

        const aiResponse = chatResponse.choices[0].message.content;
        context.res = { status: 200, body: { message: aiResponse } };

    } catch (error) {
        context.res = { status: 500, body: { message: "Error processing request: " + error.message } };
    }
};

/**
 * 🔍 Uses OpenAI to analyze user queries and determine dataset, column, and search value.
 */
async function analyzeUserQuery(userMessage) {
    const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

    const prompt = `
    You are an AI assistant that helps classify user queries to retrieve structured data from a set of CSV datasets.

    Available datasets:
    - "barcodes.csv" (Generic barcode for SKUs)
    - "materialBasicData.csv" (Basic SKU details: description, manufacturer, part numbers)
    - "purchaseRecords.csv" (Active purchase records: purchase order, vendor, date, delivery info)
    - "warehouseData.csv" (Stock levels, storage bins, UOM)
    - "stockTransactions.csv" (Material movements: purchases, goods issue, transfers)
    - "stockPricingData.csv" (Moving average price of SKU)
    - "reservationData.csv" (Internal reservations for SKU)

    Your task:
    - Identify which dataset should be queried.
    - Identify the column where the data should be searched.
    - Extract the search value (e.g., SKU ID, purchase order number).

    **User Query:** "${userMessage}"

    **Response Format (STRICT JSON):**
    {"dataset": "warehouseData.csv", "column": "SKU", "value": "10271"}

    If no dataset match is found, return:
    {"dataset": null, "column": null, "value": null}
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
        context.log(`📂 Checking if ${filename} exists in Blob Storage...`);
        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const containerClient = blobServiceClient.getContainerClient(DATASETS_CONTAINER);
        const blobClient = containerClient.getBlobClient(filename);

        const exists = await blobClient.exists();
        if (!exists) {
            context.log(`❌ ERROR: File ${filename} NOT FOUND.`);
            throw new Error(`File ${filename} not found.`);
        }

        context.log(`⬇️ Downloading ${filename} from Blob Storage...`);
        const downloadResponse = await blobClient.download();
        const downloadedData = await streamToString(downloadResponse.readableStreamBody);

        if (!downloadedData.trim()) {
            context.log(`❌ ERROR: File ${filename} is empty.`);
            throw new Error(`File ${filename} is empty.`);
        }

        context.log(`📄 Parsing ${filename}...`);

        return new Promise((resolve, reject) => {
            let results = [];
            csv.parseString(downloadedData, { headers: true, trim: true })
                .on("headers", (headerList) => {
                    context.log(`✅ CSV Headers: ${headerList}`);
                    if (!headerList.includes(column)) {
                        reject(new Error(`Column '${column}' not found in ${filename}.`));
                    }
                })
                .on("data", (row) => {
                    context.log(`🔹 Row Data: ${JSON.stringify(row)}`); // Log full row data

                    if (row[column] && row[column].toLowerCase().includes(value.toLowerCase())) {
                        results.push(row);
                        context.log(`✅ Match Found: ${JSON.stringify(row)}`);
                    }
                })
                .on("end", () => {
                    context.log(`✅ Found ${results.length} matching records in ${filename}`);
                    resolve(results);
                })
                .on("error", (err) => {
                    reject(new Error(`CSV Parsing Failed: ${err.message}`));
                });
        });
    } catch (error) {
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
