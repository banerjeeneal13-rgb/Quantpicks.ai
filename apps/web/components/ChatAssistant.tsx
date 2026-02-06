"use client";

import { useState } from "react";

type ChatMessage = {
  role: "user" | "assistant";
  content: string;
};

export default function ChatAssistant() {
  const [messages, setMessages] = useState<ChatMessage[]>([
    {
      role: "assistant",
      content:
        "Ask about picks, EV, or filters. This is a safe beta assistant and does not guarantee outcomes.",
    },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);

  const send = async () => {
    const text = input.trim();
    if (!text) return;
    setInput("");
    const next = [...messages, { role: "user" as const, content: text }] as ChatMessage[];
    setMessages(next);
    setLoading(true);
    try {
      const res = await fetch("/api/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ messages: next }),
      });
      const json = await res.json();
      const reply = json?.message || "Assistant is not configured yet.";
      setMessages([...next, { role: "assistant" as const, content: reply }]);
    } catch {
      setMessages([...next, { role: "assistant" as const, content: "Assistant is offline." }]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="border rounded-2xl p-4 space-y-4">
      <div className="space-y-2 max-h-[420px] overflow-auto">
        {messages.map((m, i) => (
          <div
            key={i}
            className={`rounded-xl px-3 py-2 text-sm ${
              m.role === "assistant" ? "bg-black/5" : "bg-black/10"
            }`}
          >
            <div className="font-semibold text-xs uppercase opacity-60">{m.role}</div>
            <div>{m.content}</div>
          </div>
        ))}
      </div>

      <div className="flex gap-2">
        <input
          className="flex-1 border rounded-xl px-3 py-2 text-sm bg-transparent"
          placeholder="Ask about a player or market..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => (e.key === "Enter" ? send() : null)}
        />
        <button
          className="border rounded-xl px-3 py-2 text-sm disabled:opacity-50"
          onClick={send}
          disabled={loading}
        >
          {loading ? "..." : "Send"}
        </button>
      </div>
    </div>
  );
}
