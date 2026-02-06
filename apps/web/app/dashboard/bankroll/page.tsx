import BankrollTool from "@/components/BankrollTool";

export default function BankrollPage() {
  return (
    <main className="mx-auto max-w-6xl p-6 space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold">Bankroll Manager</h1>
        <p className="text-sm opacity-70">Set your unit size and max bet in seconds.</p>
      </header>

      <BankrollTool />
    </main>
  );
}
