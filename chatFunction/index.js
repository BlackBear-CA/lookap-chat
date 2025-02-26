/* ========== MODULE IMPORTS AND ENV CONFIG ========== */
// Description: Core dependencies and environment configuration
const { BlobServiceClient } = require("@azure/storage-blob");
const OpenAI = require("openai");
const csv = require("fast-csv");

// Description: Environment variable configuration
// - AZURE_STORAGE_CONNECTION_STRING: Connection string for Azure Blob Storage
// - OPENAI_API_KEY: OpenAI API authentication key
// - DATASETS_CONTAINER: Fixed container name for dataset files
const ENV = {
  AZURE_STORAGE_CONNECTION_STRING: process.env.AZURE_STORAGE_CONNECTION_STRING,
  OPENAI_API_KEY: process.env.OPENAI_API_KEY,
  DATASETS_CONTAINER: "datasets"
};

/* ========== ENVIRONMENT VALIDATION ========== */
// Description: Immediate validation of required environment variables
// Executes on module load to fail fast if config is missing
(() => {
  if (!ENV.AZURE_STORAGE_CONNECTION_STRING || !ENV.OPENAI_API_KEY) {
    throw new Error("Missing required environment variables");
  }
})();

/* ========== AI DATA SERVICE CLASS ========== */
// Description: Handles all AI-related operations including:
// - Query analysis using OpenAI
// - Response parsing and error handling
class AIDataService {
  constructor() {
    // Initialize OpenAI client with API key
    this.openai = new OpenAI({ apiKey: ENV.OPENAI_API_KEY });
    // Create system prompt for query analysis
    this.ANALYSIS_PROMPT = this.createAnalysisPrompt();
  }

  // Description: Defines the structured prompt for query analysis
  // Guides AI to identify datasets, columns, and search values
  createAnalysisPrompt() {
    return `
    You are an AI data assistant responsible for analyzing user queries 
    and identifying relevant datasets, columns, and search values.

    ### Response Format:
    Respond ONLY in valid JSON format. Use this exact structure:
    {
      "dataset": "filename.csv",
      "columns": ["column1", "column2"],
      "value": "search_term",
      "confidence": 0.0-1.0
    }

    ### Additional Rules:
    - If the dataset or columns cannot be identified, return:
      {
        "dataset": null,
        "columns": [],
        "value": null,
        "confidence": 0.0
      }
    - NEVER include explanations or additional text outside the JSON structure.
    `;
  }

// Description: Main query analysis workflow
// 1. Sends query to OpenAI
// 2. Processes response
// 3. Handles errors gracefully with a timeout mechanism

async analyzeQuery(userMessage, context) {
    try {
        context.log("Initializing query analysis...");

        // Set a timeout for OpenAI API request (15 seconds max)
        const timeoutLimit = 15000; // 15 seconds
        const timeoutPromise = new Promise((_, reject) => 
            setTimeout(() => reject(new Error("OpenAI request timeout")), timeoutLimit)
        );

        const openAIRequest = this.openai.chat.completions.create({
            model: "gpt-4",
            temperature: 0.2,  // Reduce randomness for structured responses
            max_tokens: 200,    // Limit response size
            messages: [
                { role: "system", content: this.ANALYSIS_PROMPT },
                { role: "user", content: userMessage }
            ]
        });

        // Race between OpenAI request and timeout
        const response = await Promise.race([openAIRequest, timeoutPromise]);

        if (!response || !response.choices || response.choices.length === 0) {
            throw new Error("OpenAI returned an empty response");
        }

        const responseText = response.choices[0]?.message?.content?.trim();
        if (!responseText) {
            throw new Error("OpenAI response is empty or malformed");
        }

        context.log(`Raw AI Response: ${responseText}`);

        // Ensure response is in JSON format
        let parsedResponse;
        try {
            parsedResponse = JSON.parse(responseText);
        } catch (jsonError) {
            throw new Error(`Failed to parse OpenAI response as JSON: ${responseText}`);
        }

        // Validate parsed JSON structure
        if (!parsedResponse.dataset || !parsedResponse.columns || !parsedResponse.value) {
            throw new Error(`Incomplete data received from OpenAI: ${JSON.stringify(parsedResponse)}`);
        }

        return { ...parsedResponse, isValid: true };
        
    } catch (error) {
        context.log(`AI Analysis Error: ${error.message}`);
        return {
            isValid: false,
            fallback: "I'm having trouble processing your request. Please try again later."
        };
    }
}

  // Description: Parses raw AI response into structured data
  // Extracts dataset, columns, and search value
  // Handles JSON parsing and validation
  parseOpenAIResponse(response, context) {
    try {
      const responseText = response.choices[0]?.message?.content || '';
      context.log(`Sanitized AI Response: ${responseText.substring(0, 100)}...`);
  
      // New: Check for JSON structure before parsing
      if (!responseText.startsWith('{') && !responseText.includes('dataset')) {
        throw new Error('Non-JSON response format detected');
      }

        // Attempt to parse structured JSON response
        const parsedResponse = JSON.parse(responseText);

        const result = {
            dataset: parsedResponse.dataset || null,
            columns: Array.isArray(parsedResponse.columns) 
                ? parsedResponse.columns.map(c => c.trim()).filter(Boolean) 
                : [],
            value: parsedResponse.value || null,
            fallback: "I'm not sure about that query. Could you clarify?",
        };

        return result.dataset && result.columns.length > 0 && result.value 
            ? { ...result, isValid: true } 
            : { ...result, isValid: false };

    } catch (error) {
        context.log(`Error parsing OpenAI response: ${error.message}`);
        return {
            isValid: false,
            fallback: "I'm having trouble processing your request. Please try again."
        };
    }
}

  // Description: Error handler for AI operations
  // Logs errors and returns fallback message
  handleAnalysisError(error, context) {
    context.log(`Analysis Error: ${error.message}`);
    return {
        isValid: false,
        fallback: `I couldn't process "${userMessage}". Can you provide more details?`
      };      
  }
}

/* ========== BLOB DATA SERVICE CLASS ========== */
// Description: Manages Azure Blob Storage operations including:
// - Dataset file retrieval
// - CSV processing and filtering
class BlobDataService {
  constructor() {
    // Initialize Azure Blob Service client
    this.serviceClient = BlobServiceClient.fromConnectionString(
      ENV.AZURE_STORAGE_CONNECTION_STRING
    );
    this.columnMappings = {
        'soh uom': ['soh', 'uom'],
        'rop maxStock mrpType': ['rop', 'maxStock', 'mrpType'],
        'sku_id item_description': ['sku_id', 'item_description']
      };
  }

  // Description: Main dataset query workflow
  // 1. Verifies blob existence
  // 2. Streams CSV data
  // 3. Processes results
  async queryDataset(context, filename, columns, searchValue) {
    try {
      const containerClient = this.serviceClient.getContainerClient(ENV.DATASETS_CONTAINER);
      const blobClient = containerClient.getBlobClient(filename);

      if (!await blobClient.exists()) {
        throw new Error(`Dataset ${filename} not found`);
      }

      const dataStream = await this.getDataStream(blobClient);
      return this.processCSVData(dataStream, columns, searchValue, context);
    } catch (error) {
      context.log(`Dataset Error: ${error.message}`);
      throw error;
    }
  }

  // Description: Retrieves readable stream from blob storage
  async getDataStream(blobClient) {
    const downloadResponse = await blobClient.download();
    return downloadResponse.readableStreamBody;
  }

  // Description: CSV processing pipeline
  // 1. Validates columns
  // 2. Filters rows by search value
  // 3. Returns matching results
  async processCSVData(stream, columns, value, context) {
    return new Promise((resolve, reject) => {
      const results = [];
      let normalizedHeaders = [];
      const parser = csv.parseStream(stream, {
        headers: headers => this.normalizeHeaders(headers),
        trim: true
      });

      parser
      .on("headers", headers => {
        normalizedHeaders = headers;
        this.validateColumns(headers, columns, context);
      })
      .on("data", row => this.processNormalizedRow(row, normalizedHeaders, columns, value, results, context))
      .on("end", () => resolve(results))
      .on("error", error => reject(error));
  });
}

  // Description: Validates requested columns against CSV headers
  validateColumns(headers, targetColumns, context) {
    const validColumns = targetColumns.filter(col => headers.includes(col));
    if (validColumns.length === 0) {
      throw new Error(`No valid columns found in: ${headers.join(", ")}`);
    }
    context.log(`Valid columns: ${validColumns.join(", ")}`);
  }

  // Description: Processes individual CSV rows
  // Applies case-insensitive search across specified columns
  processRow(row, columns, value, results, context) {
    const searchValue = value.toLowerCase();
    for (const col of columns) {
      const cellValue = (row[col] || "").toString().toLowerCase();
      if (cellValue.includes(searchValue)) {
        results.push(row);
        context.log(`Match found in column ${col}: ${cellValue}`);
        break;
      }
    }
  }
}

/* ========== RESPONSE FORMATTER CLASS ========== */
// Description: Transforms raw data into user-friendly responses
// Handles multiple result scenarios and empty states
class ResponseFormatter {
    // Description: Main formatting entry point
    static format(results, columns, value, context) {
      return results.length === 0
        ? this.noResultsResponse(value)
        : this.resultsResponse(results, columns, context);
    }
  
    // Description: Handles empty result scenario
    static noResultsResponse(value) {
      return `No records found for '${value}'. Would you like to try a different search?`;
    }
  
    // Description: Routes to appropriate response formatter
    static resultsResponse(results, columns, context) {
      if (results.length > 1) {
        return this.multiResultResponse(results, columns);
      }
      return this.singleResultResponse(results[0], columns.length > 0 ? columns[0] : Object.keys(results[0])[0]);
    }
  
    // Description: Formats multiple results as numbered list
    static multiResultResponse(results, columns) {
      const primaryColumn = columns.length > 0 ? columns[0] : Object.keys(results[0])[0] || "Unknown";
      return results.map((row, index) => 
        `${index + 1}. SKU ${row.sku_id || "N/A"} - ${row[primaryColumn] || "Unknown"}`
      ).join("\n") + "\nPlease specify which item you need details for.";
    }
  
    // Description: Formats single result with primary column focus
    static singleResultResponse(row, primaryColumn) {
      return `${primaryColumn}: ${row[primaryColumn] || "N/A"}`;
    }
  }

/* ========== AZURE FUNCTION ENTRY POINT ========== */
// Description: Main function handler for Azure Functions
// Orchestrates query processing workflow with timeout protection:
// 1. Logs request details for debugging slow queries
// 2. Sets up timeout promise (8 seconds or ENV setting)
// 3. Runs main processing logic race against timeout
// 4. Returns appropriate responses or errors

module.exports = async function (context, req) {
    const startTime = Date.now(); // Start timer to measure execution time
    context.log(`Received request at ${new Date().toISOString()}`);

    const aiService = new AIDataService();
    const blobService = new BlobDataService();
    
    // Set timeout (default 8s or custom from ENV)
    const timeoutLimit = ENV.RESPONSE_TIMEOUT || 8000; 
    const timeoutPromise = new Promise((_, reject) => 
        setTimeout(() => reject(new Error(`Timeout after ${timeoutLimit}ms`)), timeoutLimit)
    );

    try {
        const result = await Promise.race([
            processRequest(context, req, aiService, blobService),
            timeoutPromise
        ]);

        const executionTime = Date.now() - startTime; // Calculate execution time
        context.log(`Execution completed in ${executionTime}ms`);

        return result;
    } catch (error) {
        const executionTime = Date.now() - startTime; // Capture time before error
        context.log(`Function Error: ${error.stack}`);
        context.log(`Total Execution Time before failure: ${executionTime}ms`);

        return {
            status: 500,
            body: { 
                error: "Processing failed",
                details: error.message.includes("Timeout") 
                    ? "Try using specific inventory terms like SKU numbers or product names" 
                    : error.message,
                support: "contact@inventory-support.com",
                executionTime: `${executionTime}ms`
            }
        };
    }
};

// Main processing logic extracted for clarity
async function processRequest(context, req, aiService, blobService) {
    // Validate input
    const userMessage = req.body?.userMessage?.trim();
    if (!userMessage) {
      return { status: 400, body: { error: "Missing userMessage" } };
    }
  
    // Analyze query with AI
    const analysis = await aiService.analyzeQuery(userMessage, context);
    
    // Handle valid dataset query
    if (analysis.isValid) {
      const results = await blobService.queryDataset(
        context, 
        analysis.dataset, 
        analysis.columns, 
        analysis.value
      );
      
      return {
        status: 200,
        headers: { "Content-Type": "application/json" },  // Ensure response is recognized as JSON
        body: JSON.stringify({
            message: ResponseFormatter.format(results, analysis.columns, analysis.value, context) || "No response generated."
        })
    };
} 
// Fallback to AI-generated response
const openaiResponse = await aiService.openai.chat.completions.create({
    model: "gpt-4",
    messages: [{ role: "user", content: userMessage }],
    max_tokens: 150
});
}
return {
    status: 200,
    headers: { "Content-Type": "application/json" },  // Ensure JSON enforcement
    body: JSON.stringify({
        message: openaiResponse.choices[0]?.message?.content || analysis.fallback
    })
};