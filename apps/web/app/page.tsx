import HomeDashboard from "@/components/HomeDashboard";
import Link from "next/link";

export default function Home() {
  return (
    <main className="p-10 max-w-6xl mx-auto space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-4xl font-semibold">Quantpicks.ai</h1>
          <p className="mt-2 text-base opacity-80">
            NBA props, match lines, and bankroll tracking driven by model probabilities.
          </p>
        </div>
        <div className="flex gap-3">
          <Link className="px-5 py-3 rounded-xl shadow border" href="/dashboard/props">
            Open Dashboard
          </Link>
          <Link className="px-5 py-3 rounded-xl shadow border" href="/pricing">
            Pricing
          </Link>
        </div>
      </div>

      <HomeDashboard />

      <p className="text-sm opacity-60">
        Disclaimer: This is an informational analytics tool. No guarantees of profit.
      </p>
    </main>
  );
}
