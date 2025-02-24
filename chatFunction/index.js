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

    const skuMatch = userMessage.match(/sku\s+(\d+)/i);
    const pumpMatch = userMessage.match(/where do we buy this pump/i);

    try {
        if (skuMatch) {
            let skuId = skuMatch[1];
            context.log(`🔎 SKU Query Detected: ${skuId}`);

            let purchaseData = await searchDataset(context, "purchaseRecords.csv", "sku_id", skuId);
            context.log("📄 Purchase Data Found:", JSON.stringify(purchaseData));

            if (purchaseData.length > 0) {
                const record = purchaseData[0];
                const responseMessage = `Yes, there is a purchase order (${record.purchaseOrd}) for SKU ${skuId} with vendor ${record.vendorName}, ordered on ${record.doc_creation_date}, and delivery is expected on ${record.delivery_date}.`;

                context.log("✅ Responding with SKU details:", responseMessage);
                context.res = { status: 200, body: { message: responseMessage } };
                return;
            } else {
                context.log(`⚠️ No purchase order found for SKU ${skuId}.`);
                context.res = { status: 200, body: { message: `No purchase order found for SKU ${skuId}.` } };
                return;
            }
        }

        if (pumpMatch) {
            context.log("🔎 Pump Query Detected");

            let purchaseData = await searchDataset(context, "purchaseRecords.csv", "item_description", "pump");
            context.log("📄 Pump Purchase Data:", JSON.stringify(purchaseData));

            if (purchaseData.length > 0) {
                const record = purchaseData[0];
                const responseMessage = `We buy this pump from **${record.vendorName}** under Purchase Order **${record.purchaseOrd}**, ordered on **${record.doc_creation_date}**, with expected delivery on **${record.delivery_date}**.`;

                context.log("✅ Responding with Pump details:", responseMessage);
                context.res = { status: 200, body: { message: responseMessage } };
                return;
            } else {
                context.log("⚠️ No purchase records found for a pump.");
                context.res = { status: 200, body: { message: `No purchase records found for a pump.` } };
                return;
            }
        }

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

async function searchDataset(context, filename, column, value) {
    try {
        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const containerClient = blobServiceClient.getContainerClient(DATASETS_CONTAINER);
        const blobClient = containerClient.getBlobClient(filename);

        context.log(`📂 Checking existence of ${filename} in Blob Storage...`);
        const exists = await blobClient.exists();
        if (!exists) {
            context.log(`❌ ERROR: File ${filename} NOT FOUND.`);
            throw new Error(`File ${filename} not found.`);
        }

        context.log(`⬇️ Downloading ${filename} from Blob Storage...`);
        const downloadResponse = await blobClient.download();
        const downloadedData = await streamToString(downloadResponse.readableStreamBody);

        context.log(`📄 File ${filename} successfully downloaded, parsing CSV...`);

        return new Promise((resolve, reject) => {
            let results = [];

            csv.parseString(downloadedData, { headers: true })
                .on("data", (row) => {
                    if (row[column] && typeof row[column] === "string") {
                        if (row[column].toLowerCase().includes(value.toLowerCase())) {
                            results.push(row);
                        }
                    } else {
                        context.log(`⚠️ Column '${column}' missing in row:`, row);
                    }
                })
                .on("end", () => {
                    context.log(`✅ Found ${results.length} matching records for '${value}' in ${filename}`);
                    resolve(results);
                })
                .on("error", (err) => {
                    context.log(`❌ ERROR: CSV Parsing Failed: ${err.message}`);
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
