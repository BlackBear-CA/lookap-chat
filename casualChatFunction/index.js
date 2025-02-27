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
    context.log("Received a request for casualChatFunction");

    const userMessage = req.body?.userMessage || "";
    
    if (!userMessage) {
        context.log("Error: No userMessage received.");
        context.res = {
            status: 400,
            body: JSON.stringify({ error: "Missing userMessage in request." })
        };
        return;
    }

    try {
        // Sample AI response (Replace with OpenAI call if implemented)
        const aiResponse = `You said: "${userMessage}". I'm here to assist you!`;

        context.log("AI Response:", aiResponse);

        // Ensure response is returned as JSON
        context.res = {
            status: 200,
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ success: true, message: aiResponse })
        };

    } catch (error) {
        context.log("Error in processing:", error.message);
        context.res = {
            status: 500,
            body: JSON.stringify({ error: "Internal server error", details: error.message })
        };
    }
};

