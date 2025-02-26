const { BlobServiceClient } = require("@azure/storage-blob");
const OpenAI = require("openai");
const csv = require("fast-csv");

// Environment Configuration
const ENV = {
  AZURE_STORAGE_CONNECTION_STRING: process.env.AZURE_STORAGE_CONNECTION_STRING,
  OPENAI_API_KEY: process.env.OPENAI_API_KEY,
  DATASETS_CONTAINER: "datasets"
};

// Validate environment on load
(() => {
  if (!ENV.AZURE_STORAGE_CONNECTION_STRING || !ENV.OPENAI_API_KEY) {
    throw new Error("Missing required environment variables");
  }
})();

class AIDataService {
  constructor() {
    this.openai = new OpenAI({ apiKey: ENV.OPENAI_API_KEY });
    this.ANALYSIS_PROMPT = this.createAnalysisPrompt();
  }

  createAnalysisPrompt() {
    return `
    [System prompt content identical to original...]
    `;
  }

  async analyzeQuery(userMessage, context) {
    try {
      context.log("Initializing query analysis...");
      const response = await this.openai.chat.completions.create({
        model: "gpt-4",
        messages: [{ role: "system", content: this.ANALYSIS_PROMPT }]
      });

      return this.parseOpenAIResponse(response, context);
    } catch (error) {
      context.log(`AI Analysis Error: ${error.stack}`);
      return this.handleAnalysisError(error, context);
    }
  }

  parseOpenAIResponse(response, context) {
    const responseText = response.choices[0]?.message?.content || '';
    context.log(`Raw AI Response: ${responseText}`);

    const datasetMatch = responseText.match(/Dataset:\s*([\w.]+\.csv)/i);
    const columnsMatch = responseText.match(/Columns?:\s*\[?([\w,\s-]+)\]?/i);
    const valueMatch = responseText.match(/Value:\s*([\w\d]+)/i);

    const result = {
      dataset: datasetMatch?.[1],
      columns: columnsMatch?.[1]?.split(",").map(c => c.trim()).filter(Boolean),
      value: valueMatch?.[1],
      fallback: "I'm not sure about that query. Could you clarify?",
    };

    return result.dataset && result.columns && result.value 
      ? { ...result, isValid: true } 
      : { ...result, isValid: false };
  }

  handleAnalysisError(error, context) {
    context.log(`Analysis Error: ${error.message}`);
    return {
      isValid: false,
      fallback: "I'm having trouble processing your request. Please try again."
    };
  }
}

class BlobDataService {
  constructor() {
    this.serviceClient = BlobServiceClient.fromConnectionString(
      ENV.AZURE_STORAGE_CONNECTION_STRING
    );
  }

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

  async getDataStream(blobClient) {
    const downloadResponse = await blobClient.download();
    return downloadResponse.readableStreamBody;
  }

  async processCSVData(stream, columns, value, context) {
    return new Promise((resolve, reject) => {
      const results = [];
      const parser = csv.parseStream(stream, { headers: true, trim: true });

      parser
        .on("headers", headers => this.validateColumns(headers, columns, context))
        .on("data", row => this.processRow(row, columns, value, results, context))
        .on("end", () => resolve(results))
        .on("error", error => reject(error));
    });
  }

  validateColumns(headers, targetColumns, context) {
    const validColumns = targetColumns.filter(col => headers.includes(col));
    if (validColumns.length === 0) {
      throw new Error(`No valid columns found in: ${headers.join(", ")}`);
    }
    context.log(`Valid columns: ${validColumns.join(", ")}`);
  }

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

class ResponseFormatter {
  static format(results, columns, value, context) {
    return results.length === 0
      ? this.noResultsResponse(value)
      : this.resultsResponse(results, columns, context);
  }

  static noResultsResponse(value) {
    return `No records found for '${value}'. Would you like to try a different search?`;
  }

  static resultsResponse(results, columns, context) {
    if (results.length > 1) {
      return this.multiResultResponse(results, columns);
    }
    return this.singleResultResponse(results[0], columns[0]);
  }

  static multiResultResponse(results, columns) {
    const primaryColumn = columns[0];
    return results.map((row, index) => 
      `${index + 1}. SKU ${row.sku_id || "N/A"} - ${row[primaryColumn] || "Unknown"}`
    ).join("\n") + "\nPlease specify which item you need details for.";
  }

  static singleResultResponse(row, primaryColumn) {
    const responses = {
      soh: `Stock on hand: ${row[primaryColumn]}`,
      vendorName: `Supplied by: ${row[primaryColumn]}`,
      manufacturer: `Manufacturer: ${row[primaryColumn]}`,
      default: `${primaryColumn}: ${row[primaryColumn]}`
    };

    return responses[primaryColumn] || responses.default;
  }
}

// Azure Function Entry Point
module.exports = async function (context, req) {
  const aiService = new AIDataService();
  const blobService = new BlobDataService();
  
  try {
    const userMessage = req.body?.userMessage?.trim();
    if (!userMessage) return { status: 400, body: { error: "Missing userMessage" } };

    const analysis = await aiService.analyzeQuery(userMessage, context);
    
    if (analysis.isValid) {
      const results = await blobService.queryDataset(
        context, 
        analysis.dataset, 
        analysis.columns, 
        analysis.value
      );
      
      return {
        status: 200,
        body: { message: ResponseFormatter.format(results, analysis.columns, analysis.value, context) }
      };
    }

    const openaiResponse = await aiService.openai.chat.completions.create({
      model: "gpt-4",
      messages: [{ role: "user", content: userMessage }]
    });

    return {
      status: 200,
      body: { message: openaiResponse.choices[0].message.content || analysis.fallback }
    };

  } catch (error) {
    context.log(`Function Error: ${error.stack}`);
    return {
      status: 500,
      body: { error: `Processing error: ${error.message}` }
    };
  }
};