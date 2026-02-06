import Link from "next/link";

const NAV_LINKS = [
  { href: "/dashboard/props", label: "Props" },
  { href: "/dashboard/top-edges", label: "Top Edges" },
  { href: "/dashboard/live", label: "Live" },
  { href: "/", label: "Home" },
  { href: "/dashboard/manual-odds", label: "Manual Odds" },
  { href: "/dashboard/bankroll", label: "Bankroll" },
  { href: "/dashboard/chat", label: "AI Chat" },
];

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  return (
    <div>
      <nav className="p-5 border-b">
        <div className="flex flex-wrap items-center justify-center gap-6">
          {NAV_LINKS.map((link) => {
            const isHome = link.label === "Home";
            return (
              <Link
                key={link.href}
                href={link.href}
                className={
                  isHome
                    ? "text-base font-bold px-4 py-1 rounded-full border border-black/20"
                    : "text-sm font-semibold"
                }
              >
                {link.label}
              </Link>
            );
          })}
        </div>
      </nav>
      <div className="p-6 max-w-6xl mx-auto">{children}</div>
    </div>
  );
}
