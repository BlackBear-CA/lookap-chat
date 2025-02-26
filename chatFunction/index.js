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
}
    const prompt = `
    You are an AI assistant that helps users retrieve structured data from an inventory system. Users may search using SKU IDs, stock numbers, manufacturer names, part numbers, or keywords like "pump" or "filter."
    
    ### **Your Task:**
    - Identify the **dataset** that should be queried.
    - Identify the **correct column(s)** where the data should be searched.
    - Extract the **search value** that will be used for retrieval.
    
    ### **Key Definitions:**
    - **SKU ID synonyms:** "sku", "stock", "stock number", "stock nos.", "stock #", "stock code", "material nos.", "material number", "material", "material #", "stock keeping unit"
      - These should be mapped to **sku_id** when searching in datasets.
    
    ### **Dataset Definitions & Purposes:**
    - **barcodes.csv** → Contains the generic barcode assigned to each SKU.
    - **consolidated_columns.csv** → Lists all column headers and which CSV files they exist in.
    - **materialBasicData.csv** → Stores basic SKU information (SKU ID, item description, manufacturer, category, sub-category, references).
    - **missingItemReport.csv** → Logs missing items reported by employees.
    - **mrpData.csv** → Stores Material Requirements Planning (MRP) settings and configurations for SKU IDs.
    - **optimizerDataIBM.csv** → Contains SKU usage information, business impact, criticality, and stock analytics.
    - **purchaseMaster.csv** → Stores all historical purchasing records, including pricing.
    - **purchaseRecords.csv** → Stores **active purchase orders**, including vendor details.
    - **recommendedOrders.csv** → Lists **planned purchase orders** awaiting approval.
    - **reservationData.csv** → Tracks **internal reservations** of SKUs.
    - **stockCategoryReference.csv** → Maps SKU categories and subcategories.
    - **stockLogisticsData.csv** → Stores **shipment references and logistics details**.
    - **stockMaintenanceData.csv** → Shows where the SKU is **used or assigned**.
    - **stockOwnerReference.csv** → Lists **who uses the material**.
    - **stockPricingData.csv** → Stores the **moving average price** for SKUs.
    - **stockTransactions.csv** → Contains **detailed material movements**, including purchases, transfers, and goods issues.
    - **subscriberData.csv** → Logs **users who subscribe to alerts** for SKU activity.
    - **tableDirectory.csv** → Maps CSV files to their table IDs and descriptions.
    - **warehouseData.csv** → Stores SKU **stock levels, bin locations, and unit of measure**.
    
### **Example Queries & Expected Outputs:**
- **User:** "I need a SKU ID for a pump"
  - **Dataset:** "materialBasicData.csv"
  - **Columns:** ["item_description"]
  - **Value:** "pump"

- **User:** "Where do we buy stock 10271?"
  - **Dataset:** "purchaseRecords.csv"
  - **Columns:** ["vendorName", "vendorID"]
  - **Value:** "10271"

- **User:** "How many are in stock for material number 10271?"
  - **Dataset:** "warehouseData.csv"
  - **Columns:** ["soh"]
  - **Value:** "10271"

---
**Response Format:**  
Reply conversationally but include structured data inside `<response>` tags.

💡 **Example Output:**
"Here’s what I found:  
- Dataset: <response>materialBasicData.csv</response>  
- Columns: <response>["item_description"]</response>  
- Value: <response>pump</response>  
Does this help?"
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

    // Log raw response for debugging
    const responseText = response.choices[0].message.content;
    context.log(`📩 OpenAI Raw Response: ${responseText}`);

    // Attempt to extract dataset, column, and value dynamically
    const datasetMatch = responseText.match(/Dataset:\s*([\w.]+\.csv)/i);
    const columnMatch = responseText.match(/Column:\s*([\w]+)/i);
    const valueMatch = responseText.match(/Value:\s*([\w\d]+)/i);

    const dataset = datasetMatch ? datasetMatch[1] : null;
    const column = columnMatch ? columnMatch[1] : null;
    const value = valueMatch ? valueMatch[1] : null;

    if (!dataset || !column || !value) {
        context.log("⚠️ OpenAI response missing dataset, column, or value:", responseText);
        return { dataset: null, column: null, value: null };
    }

    return { dataset, column, value };

} catch (error) {
    context.log(`❌ OpenAI API Error: ${error.message}`);
    return { dataset: null, column: null, value: null };
}

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
            let allHeaders = [];
        
            csv.parseString(downloadedData, { headers: true, trim: true })
                .on("headers", (headers) => {
                    allHeaders = headers;
                    context.log(`✅ CSV Headers in ${filename}: ${headers.join(", ")}`);
        
                    // Ensure at least one of the target columns exists
                    const validColumns = columns.filter(col => headers.includes(col));
        
                    if (validColumns.length === 0) {
                        context.log(`❌ None of the required columns were found in ${filename}. Available columns: ${headers.join(", ")}`);
                        reject(new Error(`Required columns missing in ${filename}.`));
                        return;
                    }
                })
                .on("data", (row) => {
                    for (const column of columns) {
                        if (row[column] && row[column].toString().toLowerCase().includes(value.toLowerCase())) {
                            results.push(row);
                            break; // Stop checking once a match is found
                        }
                    }
                })
                .on("end", () => resolve(results))
                .on("error", reject);
               
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
        }
     catch (error) {
        context.log(`❌ Error processing dataset ${filename}: ${error.message}`);
        throw new Error(`Error processing dataset ${filename}: ${error.message}`);
    }
}

/**
 * 📜 Converts the results into a conversational response.
 */
function formatResults(results, column) {
    if (!Array.isArray(results) || results.length === 0) {
        return "I couldn't find any matching records. Would you like me to check something else?";
    }

    // Handle multiple results dynamically
    if (results.length > 1) {
        let response = "Here are some matching results:\n";
        results.forEach((row, index) => {
            const sku = row.sku_id || "Unknown SKU";
            const description = row.item_description || "No description available";

            switch (column) {
                case "vendorName":
                    response += `${index + 1}. SKU ${sku} is purchased from ${row.vendorName || "Unknown Vendor"}.\n`;
                    break;
                case "manufacturer":
                    response += `${index + 1}. SKU ${sku} is manufactured by ${row.manufacturer || "Unknown Manufacturer"}.\n`;
                    break;
                case "soh":
                    response += `${index + 1}. SKU ${sku} has ${row.soh || "0"} units in stock.\n`;
                    break;
                default:
                    response += `${index + 1}. SKU ${sku} - ${description}\n`;
                    break;
            }
        });
        return response + "Let me know if you need details on a specific item.";
    }

    // Handle single result
    const row = results[0];
    const requestedValue = row[column] || "Unknown";

    // Context-aware responses
    switch (column) {
        case "soh":
            return `The current stock on hand for SKU ${row.sku_id} is ${requestedValue}. Let me know if you need more details.`;
        case "vendorName":
            return `SKU ${row.sku_id} is typically purchased from ${requestedValue}. Let me know if you need supplier details.`;
        case "order_qty":
            return `The last purchase order for SKU ${row.sku_id} was for ${requestedValue} units. Would you like to see more order history?`;
        case "manufacturer":
            return `SKU ${row.sku_id} is manufactured by ${requestedValue}. Let me know if you need technical details or specifications.`;
        case "storage_bin":
            return `This item is stored in bin ${requestedValue}. Do you need help locating it?`;
        case "unit_price":
            return `The most recent price for SKU ${row.sku_id} was $${requestedValue} per unit. Let me know if you need pricing history.`;
        default:
            return `The requested information for ${column} is ${requestedValue}. Let me know if you need anything else.`;
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