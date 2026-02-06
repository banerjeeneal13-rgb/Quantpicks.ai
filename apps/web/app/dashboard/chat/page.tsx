import ChatAssistant from "@/components/ChatAssistant";

export default function ChatPage() {
  return (
    <main className="mx-auto max-w-6xl p-6 space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold">AI Chat Assistant</h1>
        <p className="text-sm opacity-70">
          Ask about EV, props, or filters. This is informational only.
        </p>
      </header>

      <ChatAssistant />
    </main>
  );
}
