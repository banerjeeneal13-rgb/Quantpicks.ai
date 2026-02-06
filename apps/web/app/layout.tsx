import "./globals.css";
import Link from "next/link";
import { Inter } from "next/font/google";

const inter = Inter({ subsets: ["latin"] });

export const metadata = {
  title: "Quantpicks.ai",
  description: "NBA props value scanner",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body className={inter.className}>
        <header className="border-b">
          <nav className="mx-auto max-w-6xl p-4 flex items-center justify-between gap-4">
            <Link href="/" className="font-semibold">
              Quantpicks.ai
            </Link>
            <div className="flex items-center gap-3">
              <Link href="/pricing" className="text-sm font-semibold">
                View Plans
              </Link>
              <Link href="/login" className="text-sm font-semibold border rounded-full px-3 py-1">
                Login
              </Link>
            </div>
          </nav>
        </header>
        {children}
      </body>
    </html>
  );
}
