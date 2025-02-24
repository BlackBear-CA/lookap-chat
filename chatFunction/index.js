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
        context.res = {
            status: 400,
            body: { message: "Error: No userMessage found in request body." }
        };
        return;
    }

    // ✅ Improved SKU Regex Matching
    const skuMatch = userMessage.match(/\bsku(?:_id)?\s*(\d+)/i);

    try {
        if (skuMatch) {
            let skuId = skuMatch[1];
            context.log(`🔎 SKU Query Detected: ${skuId}`);

            try {
                context.log(`🔎 Calling searchDataset() for SKU: ${skuId}`);
                let purchaseData = await searchDataset(context, "purchaseRecords.csv", "sku_id", skuId);
                context.log("📄 Purchase Data Found:", JSON.stringify(purchaseData));

                if (purchaseData.length > 0) {
                    const record = purchaseData[0];
                    const responseMessage = `Yes, there is a purchase order (${record.purchaseord}) for SKU ${skuId} with vendor ${record.vendorname}, ordered on ${record.doc_creation_date}, and delivery is expected on ${record.delivery_date}.`;
                    
                    context.log("✅ Responding with SKU details:", responseMessage);
                    context.res = { status: 200, body: { message: responseMessage } };
                    return;
                } else {
                    context.log(`⚠️ No purchase order found for SKU ${skuId}.`);
                    context.res = { status: 200, body: { message: `No purchase order found for SKU ${skuId}.` } };
                    return;
                }
            } catch (error) {
                context.log("❌ ERROR: searchDataset() failed:", error.message);
                context.res = { status: 500, body: { message: "Error searching dataset: " + error.message } };
                return;
            }
        }

        // ✅ Ensure OpenAI is only used as a fallback
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

// ✅ Fix: Ensure searchDataset() handles missing columns properly
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

        // ✅ Ensure data is valid before parsing
        if (!downloadedData || downloadedData.trim() === "") {
            throw new Error(`File ${filename} is empty or unreadable.`);
        }

        context.log(`📄 Parsing ${filename}...`);

        return new Promise((resolve, reject) => {
            let results = [];
            let headers = [];
            
            csv.parseString(downloadedData, { headers: true, trim: true })
                .on("headers", (headerList) => {
                    // ✅ Normalize column names to lowercase
                    headers = headerList.map(h => h.trim().toLowerCase());
                    context.log("✅ Normalized Headers:", headers);

                    if (!headers.includes(column.toLowerCase())) {
                        reject(new Error(`Column '${column}' not found in CSV headers: ${headers.join(", ")}`));
                    }
                })
                .on("data", (row) => {
                    // ✅ Normalize row keys (lowercase & trimmed)
                    let normalizedRow = {};
                    Object.keys(row).forEach((key) => {
                        normalizedRow[key.trim().toLowerCase()] = row[key]?.toString().trim();
                    });

                    if (!normalizedRow[column.toLowerCase()]) {
                        context.log(`⚠️ Column '${column}' missing in row, skipping:`, normalizedRow);
                        return;
                    }

                    if (normalizedRow[column.toLowerCase()].toLowerCase().includes(value.toLowerCase())) {
                        results.push(normalizedRow);
                    }
                })
                .on("end", () => {
                    context.log(`✅ Found ${results.length} matching records for '${value}' in ${filename}`);
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

async function streamToString(stream) {
    return new Promise((resolve, reject) => {
        const chunks = [];
        stream.on("data", (chunk) => chunks.push(chunk));
        stream.on("end", () => resolve(Buffer.concat(chunks).toString()));
        stream.on("error", reject);
    });
}
