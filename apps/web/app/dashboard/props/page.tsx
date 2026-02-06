import PropsTable from "@/components/PropsTable";

export default function DashboardPropsPage() {
  return (
    <main className="mx-auto max-w-6xl p-6 space-y-6">
      <header className="space-y-1">
        <h1 className="text-2xl font-semibold">Props Dashboard</h1>
        <p className="text-sm opacity-70">
          The Source column shows whether each row came from your AI model (MODEL) or a fallback method.
        </p>
      </header>

      <PropsTable />
    </main>
  );
}
