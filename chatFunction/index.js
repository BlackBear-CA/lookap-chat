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
        // 🔎 Step 1: Identify dataset, column, and value using OpenAI
        const { dataset, column, value } = await analyzeUserQuery(userMessage, context);
        context.log(`🔍 Query Analysis Results: Dataset = ${dataset}, Column = ${column}, Value = ${value}`);

        if (dataset && column && value) {
            context.log(`📂 Fetching data from: ${dataset}, Column: ${column}, Value: ${value}`);

            try {
                // 🔎 Step 2: Query the dataset
                let searchResults = await searchDataset(context, dataset, column, value);
                context.log(`📊 Search Results: ${JSON.stringify(searchResults)}`);

                if (searchResults.length > 0) {
                    context.res = { status: 200, body: { message: formatResults(searchResults) } };
                } else {
                    context.res = { status: 200, body: { message: `I couldn’t find any records for '${value}'. Let me know if I can help with anything else!` } };
                }
                return;
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

        const aiResponse = chatResponse.choices[0]?.message?.content || "I’m not sure about that, but let me know how else I can help!";
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
You are an AI assistant that helps users query structured data from our inventory and purchasing records.
Ensure responses are conversational but still retrieve structured data when relevant.

**User Query:** "${userMessage}"

**Your Task:**
- If the user is asking about an item in stock, identify the **warehouse dataset** and return relevant stock levels.
- If the user is asking about purchasing information, return **vendor or purchasing history**.
- If the query is general, respond conversationally but **pull data when needed**.

**Example Queries & Responses:**
- **User Query:** "How many are in stock for SKU 10271?"
  **Response:** "We currently have 1 unit in stock, stored in bin CS1-11-J-END."

- **User Query:** "Where do we buy SKU 10005?"
  **Response:** "This SKU is purchased from our trusted suppliers. Want supplier details?"

- **User Query:** "Where do we buy this pump?" *(Referring to SKU: 10271)*
  **Response:** "This pump is sourced from Pioneer. Would you like me to pull up purchasing records for you?"

Your response should be conversational. If you retrieve data, format it in **natural language**.
`;

    try {
        const response = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [{ role: "system", content: prompt }]
        });

        if (!response.choices || response.choices.length === 0 || !response.choices[0].message.content) {
            context.log("⚠️ OpenAI response is empty or malformed.");
            return { dataset: null, column: null, value: null };
        }

        try {
            return JSON.parse(response.choices[0].message.content);
        } catch (parseError) {
            context.log("⚠️ Failed to parse OpenAI response. Falling back to general conversation.");
            return { dataset: null, column: null, value: null };
        }

    } catch (error) {
        context.log(`❌ OpenAI API Error: ${error.message}`);
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

        context.log(`📂 Checking if ${filename} exists in Blob Storage...`);
        if (!await blobClient.exists()) {
            throw new Error(`❌ File ${filename} not found.`);
        }

        context.log(`⬇️ Downloading ${filename} from Blob Storage...`);
        const downloadResponse = await blobClient.download();
        const downloadedData = await streamToString(downloadResponse.readableStreamBody);

        if (!downloadedData.trim()) {
            throw new Error(`⚠️ File ${filename} is empty.`);
        }

        return new Promise((resolve, reject) => {
            let results = [];
            csv.parseString(downloadedData, { headers: true, trim: true })
                .on("data", (row) => {
                    if (row[column] && row[column].toString().toLowerCase().includes(value.toLowerCase())) {
                        results.push(row);
                    }
                })
                .on("end", () => resolve(results))
                .on("error", reject);
        });
    } catch (error) {
        context.log(`❌ Error processing dataset ${filename}: ${error.message}`);
        throw new Error(`Error processing dataset ${filename}: ${error.message}`);
    }
}

/**
 * 📜 Converts the results into a conversational response.
 */
function formatResults(results) {
    if (!results.length) {
        return "Hmm... I couldn't find any matching records. Want me to check something else? 🤔";
    }

    const row = results[0];
    const sku = row.sku_id || "Unknown SKU";
    const stock = row.soh || "0";
    const unit = row.uom || "units";
    const description = row.item_description || "No description available";

    const responses = [
        `I found SKU **${sku}** (${description}). We have **${stock} ${unit}** in stock. Need anything else? 😊`,
        `SKU **${sku}** (${description}) currently has **${stock} ${unit}** in stock. Let me know if you need more details! 🚀`,
        `For SKU **${sku}** (${description}), there are **${stock} ${unit}** available. Anything else I can help with? 😊`
    ];

    return responses[Math.floor(Math.random() * responses.length)];
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
