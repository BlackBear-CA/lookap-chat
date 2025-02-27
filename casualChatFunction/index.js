const { OpenAI } = require("openai");

const ENV = {
  OPENAI_API_KEY: process.env.OPENAI_API_KEY
};

class CasualChatService {
  constructor() {
    this.openai = new OpenAI({ apiKey: ENV.OPENAI_API_KEY });
  }

  async handleChat(userMessage, context) {
    try {
      const systemPrompt = `
        You are a friendly AI assistant. Respond naturally and conversationally to the user's messages.
        You can handle greetings, general knowledge questions, casual conversation, and small talk.

        Examples:
        - User: "How are you?"
          AI: "I'm doing great, thanks for asking! How about you?"
          
        - User: "Tell me a joke."
          AI: "Why don’t skeletons fight each other? They don’t have the guts!"

        Answer like ChatGPT would in an engaging, helpful manner.
      `;

      const response = await this.openai.chat.completions.create({
        model: "gpt-4",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: userMessage }
        ]
      });

      const aiMessage = response.choices?.[0]?.message?.content || "I'm not sure how to respond to that.";
      context.log("AI Response:", aiMessage);

      return {
        status: 200,
        body: { success: true, message: aiMessage }
      };
    } catch (error) {
      context.log("Error processing chat:", error.message);
      return {
        status: 500,
        body: { error: "Something went wrong", details: error.message }
      };
    }
  }
}

module.exports = async function (context, req) {
    context.log("🔵 Received request for casualChatFunction");

    // Validate API key
    if (!ENV.OPENAI_API_KEY) {
        context.log("❌ OpenAI API Key is missing!");
        return generateResponse(500, { error: "Server misconfiguration: API Key missing" });
    }

    try {
        // Validate user input
        if (!req.body || !req.body.userMessage) {
            context.log("❌ Error: Missing user input.");
            return generateResponse(400, { error: "Missing user input." });
        }

        const userMessage = req.body.userMessage.trim();
        context.log(`📩 User Message: "${userMessage}"`);

        // Initialize OpenAI Client
        const openai = new OpenAI({ apiKey: ENV.OPENAI_API_KEY });

        // Call OpenAI API (Normal Chat)
        const openaiResponse = await openai.chat.completions.create({
            model: "gpt-4",
            messages: [{ role: "user", content: userMessage }],
            max_tokens: 200,
            temperature: 0.7
        });

        if (!openaiResponse || !openaiResponse.choices || !openaiResponse.choices[0]) {
            throw new Error("Invalid OpenAI response format.");
        }

        const aiMessage = openaiResponse.choices[0].message.content;
        context.log(`💬 OpenAI Response: "${aiMessage}"`);

        return generateResponse(200, { success: true, message: aiMessage });

    } catch (error) {
        context.log("🚨 OpenAI Request Failed:", error.message);
        return generateResponse(500, { error: "Failed to process request.", details: error.message });
    }
};

// ✅ Helper function for API responses
function generateResponse(status, body) {
    return {
        status,
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
    };
}
