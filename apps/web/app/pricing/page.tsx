import Link from "next/link";

export default function Pricing() {
  return (
    <main className="p-10 max-w-4xl mx-auto">
      <h1 className="text-3xl font-semibold">Pricing</h1>

      <div className="mt-6 grid md:grid-cols-2 gap-4">
        <div className="p-6 rounded-2xl border shadow">
          <h2 className="text-xl font-semibold">Starter</h2>
          <p className="mt-2 opacity-80">$29/month</p>
          <ul className="mt-4 list-disc pl-5 opacity-80">
            <li>Full +EV scanner</li>
            <li>Player pages</li>
            <li>Unlimited edges</li>
          </ul>
          <Link href="/dashboard/props" className="mt-6 inline-block px-4 py-2 rounded-xl border shadow">
            Open Dashboard
          </Link>
        </div>

        <div className="p-6 rounded-2xl border shadow">
          <h2 className="text-xl font-semibold">Pro</h2>
          <p className="mt-2 opacity-80">$59/month</p>
          <ul className="mt-4 list-disc pl-5 opacity-80">
            <li>Everything in Starter</li>
            <li>Real-time alerts</li>
            <li>Bankroll tools</li>
          </ul>
          <Link href="/dashboard/props" className="mt-6 inline-block px-4 py-2 rounded-xl border shadow">
            Open Dashboard
          </Link>
        </div>
      </div>
    </main>
  );
}
