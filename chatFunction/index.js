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
        const { dataset, column, value } = await analyzeUserQuery(userMessage, context);
        
        if (dataset && column && value) {
            context.log(`📂 Attempting to fetch data from: ${dataset}, Column: ${column}, Value: ${value}`);

            try {
                // 🔎 Step 2: Query the dataset
                let searchResults = await searchDataset(context, dataset, column, value);

                if (searchResults.length > 0) {
                    context.res = { status: 200, body: { message: formatResults(searchResults, context) } };
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
async function analyzeUserQuery(userMessage, context) { 
    context.log("🔍 Analyzing user query using OpenAI...");
    const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

    const prompt = `
    You are an AI assistant that helps classify user queries to retrieve structured data from a set of CSV datasets.

    ## **Available Datasets & Column Mappings:**
    - **barcodes.csv** (sku_id, barcode_uid)
    - **materialBasicData.csv** (clientID, store, sku_id, item_description, detailed_description, manufacturer, mfg_part_nos, item_main_category, item_sub_category, materialReference)
    - **missingItemReport.csv** (clientID, store, sku_id, item_description, storage_bin, soh, uom, reportedBy, reportingDate)
    - **mrpData.csv** (clientID, store, sku_id, item_description, stock_type, stock_status, mrpGRP, mrpType, mrpLot, stockCriticality, rop, maxStock, stockOwner, procurementInd, vendorLeadTime, receivingTime, materialMemo)
    - **optimizerDataIBM.csv** (clientID, store, sku_id, item_description, movingCode, mrpType, mrpLot, rop, maxStock, optimizerROP, optimizerMaxStock, stockCriticality, clientStockImpact, stocklikelihood, stockOwner, stockSegment, soh, mrpGRP, stockoutCost, currentStockValue, autoClientImpact, inTransit, consignmentSOH, monthsOfStock, monthsOfExcess, actualStockouts, surplusValue, surplusQtyCalculated)
    - **purchaseMaster.csv** (clientID, store, vendorID, sku_id, item_description, quotePrice, quoteReference, uom, currency)
    - **purchaseRecords.csv** (clientID, store, purchaseOrd, doc_type, doc_status, doc_short_text, purchasingGRP, doc_creation_date, vendorName, vendorID, sku_id, item_description, order_qty, order_unit, net_price, currency, net_order_val, delivery_date, requisition_tracking)
    - **reservationData.csv** (clientID, store, sku_id, item_description, requirement_date, reservationRef, maintenanceOrderRef, requirement_qty, goods_recipient)
    - **stockCategoryReference.csv** (sub_category, main_category)
    - **stockLogisticsData.csv** (clientID, store, sku_id, item_description, shipment_date, shipped_qty, shipping_reference, shipment_location, carrier, eta)
    - **stockMaintenanceData.csv** (clientID, store, sku_id, item_description, detailed_description, manufacturer, mfg_part_nos, fitment, bom_structure, bom_qty, bom_grp, bom_id, bom_ref, plannerGroup)
    - **stockOwnerReference.csv** (stockOwnerID, stockOwnerDescription)
    - **stockPricingData.csv** (clientID, store, sku_id, item_description, uom, unit_price, currency, price_unit, stock_group, stock_class, stock_type, lastChangeDate)
    - **stockTransactions.csv** (clientID, store, sku_id, item_description, transaction_ref, transaction_type, doc_reference, doc_type, doc_creation_date)
    - **subscriberData.csv** (clientID, store, sku_id, item_description, subscriberName, subscriberEmail)
    - **tableDirectory.csv** (tableID, tableName, previousName, tableDescription, scriptSource, lastChangeDate)
    - **warehouseData.csv** (clientID, store, sku_id, item_description, storage_bin, soh, uom, consignmentSOH, inTransit, rop, maxStock, mrpType)

 **User Query:** "${userMessage}"

    **Your Task:**
    - Identify the **dataset** that should be queried.
    - Identify the **correct column** where the data should be searched.
    - Extract the **search value** (e.g., SKU ID, purchase order number, vendor name).

    **Example Queries & Expected Outputs:**
    - **User Query:** "How many are in stock for SKU 10271?"
      **Output:** {"dataset": "warehouseData.csv", "column": "soh", "value": "10271"}
    
    - **User Query:** "Where do we buy SKU 10005?"
      **Output:** {"dataset": "purchaseRecords.csv", "column": "vendorID", "value": "10005"}
    
    **Response Format (STRICT JSON ONLY):**
    {"dataset": "warehouseData.csv", "column": "soh", "value": "10271"}

    If no dataset match is found, return:
    {"dataset": null, "column": null, "value": null}
    `;

    try {
        const response = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [{ role: "system", content: prompt }]
        });

        if (!response.choices || response.choices.length === 0) {
            context.log("⚠️ OpenAI response is empty.");
            return { dataset: null, column: null, value: null };
        }

        const responseText = response.choices[0].message.content;
        context.log(`📩 OpenAI Response: ${responseText}`);

        const parsedResponse = JSON.parse(responseText);

        if (!parsedResponse.dataset || !parsedResponse.column || !parsedResponse.value) {
            context.log("⚠️ OpenAI response missing dataset, column, or value:", parsedResponse);
            return { dataset: null, column: null, value: null };
        }

        return parsedResponse;
    } catch (error) {
        context.log(`❌ OpenAI API Error: ${error.message}`);
        return { dataset: null, column: null, value: null };
    }
}
/**
 * 📂 Queries the identified dataset for a matching record.
 */
/**
 * 📂 Queries the identified dataset for a matching record.
 */
async function searchDataset(context, filename, column, value) {
    try {
        const blobServiceClient = BlobServiceClient.fromConnectionString(AZURE_STORAGE_CONNECTION_STRING);
        const containerClient = blobServiceClient.getContainerClient(DATASETS_CONTAINER);
        const blobClient = containerClient.getBlobClient(filename);

        context.log(`📂 Checking if ${filename} exists in Blob Storage...`);
        const exists = await blobClient.exists();
        if (!exists) {
            throw new Error(`❌ File ${filename} not found in Blob Storage.`);
        }

        context.log(`⬇️ Downloading ${filename} from Blob Storage...`);
        const downloadResponse = await blobClient.download();
        const downloadedData = await streamToString(downloadResponse.readableStreamBody);

        if (!downloadedData.trim()) {
            throw new Error(`⚠️ File ${filename} is empty.`);
        }

        context.log(`📄 Parsing ${filename} with CSV headers check...`);

        return new Promise((resolve, reject) => {
            let results = [];
            let headersChecked = false;
            let allHeaders = [];

            csv.parseString(downloadedData, { headers: true, trim: true })
                .on("headers", (headers) => {
                    allHeaders = headers;
                    context.log(`✅ CSV Headers in ${filename}: ${headers.join(", ")}`);

                    if (!headers.includes(column)) {
                        context.log(`❌ Column '${column}' not found in ${filename}. Available columns: ${headers.join(", ")}`);
                        reject(new Error(`Column '${column}' not found in ${filename}.`));
                        return;
                    }
                    headersChecked = true;
                })
                .on("data", (row) => {
                    if (!headersChecked) {
                        context.log(`❌ CSV headers not properly read for ${filename}.`);
                        reject(new Error(`CSV headers not properly read for ${filename}.`));
                        return;
                    }

                    // Ensure the column exists in the row
                    if (!(column in row)) {
                        context.log(`⚠️ Skipping row due to missing column '${column}': ${JSON.stringify(row)}`);
                        return;
                    }

                    // Convert both row[column] and value to string before comparison
                    const rowValue = row[column] ? row[column].toString().trim().toLowerCase() : "";
                    const searchValue = value.toString().trim().toLowerCase();

                    if (rowValue.includes(searchValue)) {
                        context.log(`✅ Match Found: ${JSON.stringify(row)}`);
                        results.push(row);
                    }
                })
                .on("end", () => {
                    if (results.length > 0) {
                        context.log(`✅ Found ${results.length} matching records in ${filename}`);
                    } else {
                        context.log(`⚠️ No matching records found in ${filename}.`);
                    }
                    resolve(results);
                })
                .on("error", (err) => {
                    context.log(`❌ CSV Parsing Failed: ${err.message}`);
                    reject(new Error(`CSV Parsing Failed: ${err.message}`));
                });
        });
    } catch (error) {
        context.log(`❌ Error processing dataset ${filename}: ${error.message}`);
        throw new Error(`Error processing dataset ${filename}: ${error.message}`);
    }
}

/**
 * 📜 Converts the results into a conversational response.
 */
function formatResults(results, column) {
    if (!results.length) {
        return "I couldn't find any matching records. Would you like me to check something else?";
    }

    const row = results[0]; // Pick the first relevant result
    const requestedValue = row[column] || "Unknown"; // Get value based on column

    // Generate natural language response based on the requested column
    switch (column) {
        case "soh":
            return `Lookapp AI: The current stock on hand is a quantity of ${requestedValue}. Let me know if you need more details.`;
        case "storage_bin":
            return `Lookapp AI: The item is stored in bin ${requestedValue}. Do you need help locating it?`;
        case "unit_price":
            return `Lookapp AI: The most recent price for this item is $${requestedValue} per unit. Let me know if you need more pricing details.`;
        case "vendorID":
            return `Lookapp AI: This item is typically purchased from Vendor ID ${requestedValue}. Would you like supplier details?`;
        case "order_qty":
            return `Lookapp AI: The last purchase order for this item was for ${requestedValue} units. Would you like to see more order history?`;
        default:
            return `Lookapp AI: The requested information for ${column} is ${requestedValue}. Let me know if you need anything else.`;
    }
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
/**
 * Helper function to check if a query is too general.
 */
function isGeneralQuery(message) {
    const generalQueries = [
        "where do we buy this?",
        "where is this used?",
        "who supplies this?",
        "how much is this?",
        "where do we buy this pump?"
    ];
    return generalQueries.some(q => message.toLowerCase().includes(q));
}