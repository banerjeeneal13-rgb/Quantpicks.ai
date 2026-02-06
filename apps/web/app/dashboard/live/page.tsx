import LiveFeed from "@/components/LiveFeed";

export default function LivePage() {
  return (
    <main className="mx-auto max-w-6xl p-6 space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold">Live Feed</h1>
        <p className="text-sm opacity-70">Newest edges refresh every 30 seconds.</p>
      </header>

      <LiveFeed />
    </main>
  );
}
