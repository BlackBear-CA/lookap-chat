const { BlobServiceClient } = require('@azure/storage-blob');
const OpenAI = require('openai');
const fs = require('fs');
const csv = require('fast-csv');
const path = require('path');

const AZURE_STORAGE_CONNECTION_STRING = process.env.AZURE_STORAGE_CONNECTION_STRING;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const DATASETS_CONTAINER = "datasets";
const TEMP_DIR = "/tmp";  // Ensure this path is used in Azure Functions

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
    const poMatch = userMessage.match(/purchase order\s+(\d+)/i);
    const pumpMatch = userMessage.match(/where do we buy this pump/i);

    try {
        if (skuMatch) {
            let skuId = skuMatch[1];
            context.log(`🔎 SKU Query Detected: ${skuId}`);

            let purchaseData = await searchDataset("purchaseRecords.csv", "sku_id", skuId);
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

            let purchaseData = await searchDataset("purchaseRecords.csv", "item_description", "pump");
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

async function searchDataset(filename, column, value) {
    try {
        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const blobClient = blobServiceClient.getContainerClient(DATASETS_CONTAINER).getBlobClient(filename);

        context.log(`📂 Checking existence of ${filename} in Blob Storage...`);
        const exists = await blobClient.exists();
        if (!exists) {
            throw new Error(`File ${filename} not found in Blob Storage.`);
        }

        // Define the temporary path
        const csvPath = path.join(TEMP_DIR, filename);

        context.log(`⬇️ Downloading ${filename} from Blob Storage...`);
        const downloadResponse = await blobClient.download();

        return new Promise((resolve, reject) => {
            const writeStream = fs.createWriteStream(csvPath);
            downloadResponse.readableStreamBody.pipe(writeStream);

            writeStream.on("finish", async () => {
                context.log(`📄 Parsing ${filename} for column '${column}' with value '${value}'`);
                let results = [];
                fs.createReadStream(csvPath)
                    .pipe(csv.parse({ headers: true }))
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
                        context.log(`❌ Error reading CSV: ${err.message}`);
                        reject(err);
                    });
            });
        });
    } catch (error) {
        context.log("❌ Error processing dataset:", error.message);
        throw new Error(`Error processing dataset ${filename}: ${error.message}`);
    }
}
