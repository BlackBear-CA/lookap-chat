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
    context.log("Chat function triggered.");

    const userMessage = req.body && req.body.userMessage ? req.body.userMessage.trim() : null;
    if (!userMessage) {
        context.res = {
            status: 400,
            body: "Error: No userMessage found in request body."
        };
        return;
    }

    const skuMatch = userMessage.match(/sku\s+(\d+)/i);
    const poMatch = userMessage.match(/purchase order\s+(\d+)/i);
    const pumpMatch = userMessage.match(/where do we buy this pump/i);

    try {
        if (skuMatch) {
            let skuId = skuMatch[1];
            let purchaseData = await searchDataset("purchaseRecords.csv", "sku_id", skuId);
            if (purchaseData.length > 0) {
                const record = purchaseData[0];
                context.res = {
                    status: 200,
                    body: `Yes, there is a purchase order (${record.purchaseOrd}) for SKU ${skuId} with vendor ${record.vendorName}, ordered on ${record.doc_creation_date}, and delivery is expected on ${record.delivery_date}.`
                };
                return;
            } else {
                context.res = { status: 200, body: `No purchase order found for SKU ${skuId}.` };
                return;
            }
        }

        if (pumpMatch) {
            let purchaseData = await searchDataset("purchaseRecords.csv", "item_description", "pump");
            if (purchaseData.length > 0) {
                const record = purchaseData[0];
                context.res = {
                    status: 200,
                    body: `We buy this pump from **${record.vendorName}** under Purchase Order **${record.purchaseOrd}**, ordered on **${record.doc_creation_date}**, with expected delivery on **${record.delivery_date}**.`
                };
                return;
            } else {
                context.res = { status: 200, body: `No purchase records found for a pump.` };
                return;
            }
        }

        const openai = new OpenAI({ apiKey: OPENAI_API_KEY });
        const chatResponse = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [{ role: "user", content: userMessage }]
        });

        context.res = { status: 200, body: chatResponse.choices[0].message.content };
    } catch (error) {
        context.res = { status: 500, body: "Error processing request: " + error.message };
    }
};

async function searchDataset(filename, column, value) {
    try {
        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const blobClient = blobServiceClient.getContainerClient(DATASETS_CONTAINER).getBlobClient(filename);

        // Check if the file exists before attempting to download
        const exists = await blobClient.exists();
        if (!exists) {
            throw new Error(`File ${filename} not found in Blob Storage.`);
        }

        // Define the temporary path
        const csvPath = path.join(TEMP_DIR, filename);

        // Download the file
        const downloadResponse = await blobClient.download();
        return new Promise((resolve, reject) => {
            const writeStream = fs.createWriteStream(csvPath);
            downloadResponse.readableStreamBody.pipe(writeStream);

            writeStream.on("finish", async () => {
                let results = [];
                fs.createReadStream(csvPath)
                    .pipe(csv.parse({ headers: true }))
                    .on("data", (row) => {
                        if (row[column] && row[column].toLowerCase().includes(value.toLowerCase())) {
                            results.push(row);
                        }
                    })
                    .on("end", () => resolve(results))
                    .on("error", reject);
            });
        });
    } catch (error) {
        throw new Error(`Error processing dataset ${filename}: ${error.message}`);
    }
}
