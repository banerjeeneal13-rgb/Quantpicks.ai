"use client";

import { useEffect, useMemo, useState } from "react";
import { createPortal } from "react-dom";
import { coerceDecimalOdds, formatDecimalOdds } from "@/lib/odds";

const MARKET_OPTIONS: Record<string, string> = {
  all: "All Markets",
  moneyline: "Moneyline",
  spread: "Spread",
  total: "Total",
  player_points: "Points",
  player_rebounds: "Rebounds",
  player_assists: "Assists",
  player_threes: "3PT Made",
  player_points_rebounds_assists: "PRA",
  player_points_rebounds: "PR",
  player_points_assists: "PA",
  player_rebounds_assists: "RA",
  player_blocks_steals: "Stocks",
  player_steals: "Steals",
  player_blocks: "Blocks",
  player_turnovers: "Turnovers",
};

function marketLabel(market: string) {
  return MARKET_OPTIONS[market] ?? market;
}

// Confidence proxy that works even if we don't store model mean/std yet.
// 0.0 = coinflip, 1.0 = extremely confident
function confidenceFromP(p: number) {
  if (!Number.isFinite(p)) return 0;
  const c = Math.min(1, Math.max(0, Math.abs(p - 0.5) * 2));
  return c;
}

function confidenceBadge(p: number) {
  const c = confidenceFromP(p);
  if (c >= 0.7) return { text: "HIGH", cls: "bg-green-600/15 text-green-700" };
  if (c >= 0.4) return { text: "MED", cls: "bg-amber-600/15 text-amber-700" };
  return { text: "LOW", cls: "bg-black/10 text-black/70" };
}

// Set NEXT_PUBLIC_PLAN=free or pro in apps/web/.env.local
const PLAN = (process.env.NEXT_PUBLIC_PLAN || "free").toLowerCase();
const IS_PRO = PLAN === "pro";

export default function PropsTable() {
  const [market, setMarket] = useState("player_points");
  const [loading, setLoading] = useState(false);
  const [infoOpen, setInfoOpen] = useState(false);
  const [infoText, setInfoText] = useState("");
  const [mounted, setMounted] = useState(false);

  // filters
  const [q, setQ] = useState("");
  const [book, setBook] = useState("all");
  const [onlyPos, setOnlyPos] = useState(false);
  const [minEv, setMinEv] = useState(0.0);
  const [includeManual, setIncludeManual] = useState(true);

  // sorting
  const [sort, setSort] = useState<"ev" | "p">("ev");
  const [dir, setDir] = useState<"asc" | "desc">("desc");

  // pagination
  const [pageSize, setPageSize] = useState(IS_PRO ? 25 : 10);
  const [page, setPage] = useState(1);

  // server results
  const [rows, setRows] = useState<any[]>([]);
  const [total, setTotal] = useState(0);

  // free plan gating: always force 10
  useEffect(() => {
    if (!IS_PRO) {
      setPageSize(10);
      setPage(1);
    }
  }, []);

  const queryUrl = useMemo(() => {
    const params = new URLSearchParams();
    params.set("market", market);
    params.set("page", String(page));
    params.set("pageSize", String(pageSize));
    params.set("sort", sort);
    params.set("dir", dir);

    if (q.trim()) params.set("q", q.trim());
    if (book !== "all") params.set("book", book);

    params.set("onlyPos", onlyPos ? "1" : "0");
    if (minEv !== 0) params.set("evMin", String(minEv));
    if (!includeManual) params.set("includeManual", "0");

    return `/api/top-edges?${params.toString()}`;
  }, [market, page, pageSize, sort, dir, q, book, onlyPos, minEv, includeManual]);

  useEffect(() => {
    setLoading(true);
    fetch(queryUrl, { cache: "no-store" })
      .then(async (r) => {
        const txt = await r.text();
        try {
          return JSON.parse(txt);
        } catch {
          throw new Error("API returned invalid JSON: " + txt.slice(0, 200));
        }
      })
      .then((json) => {
        setRows(json.data || []);
        setTotal(Number(json.total || 0));
      })
      .catch(() => {
        setRows([]);
        setTotal(0);
      })
      .finally(() => setLoading(false));
  }, [queryUrl]);

  const prettyRows = useMemo(
    () =>
      rows.map((r) => ({
        ...r,
        marketPretty: marketLabel(String(r.market)),
        sidePretty: (() => {
          const sideRaw = String(r.side || "").toLowerCase();
          const marketRaw = String(r.market || "").toLowerCase();
          if (marketRaw === "moneyline" || marketRaw === "spread") {
            if (sideRaw === "over") return "HOME";
            if (sideRaw === "under") return "AWAY";
          }
          return String(r.side || "").toUpperCase();
        })(),
        bookPretty: String(r.book).toUpperCase(),
        pNum: Number(r.p),
        evNum: Number(r.ev),
      })),
    [rows]
  );

  const totalPages = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(page, totalPages);

  const logBet = async (row: any) => {
    const stakeStr = window.prompt("Stake amount?");
    if (!stakeStr) return;
    const stake = Number(stakeStr);
    if (!Number.isFinite(stake) || stake <= 0) return;

    const unitStr = window.prompt("Unit size (optional)?");
    const unit = unitStr ? Number(unitStr) : null;
    const odds = coerceDecimalOdds(row);
    if (odds === null) return;

    try {
      await fetch("/api/bets", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          player_name: row.player_name,
          market: row.market,
          side: row.side,
          line: row.line,
          odds,
          stake,
          unit,
          book: row.book,
          starts_at: row.starts_at,
          source: "auto",
        }),
      });
    } catch {
      // ignore
    }
  };

  const openInfo = async (row: any) => {
    try {
      const res = await fetch(`/api/edge-info?id=${encodeURIComponent(row.id)}`);
      const json = await res.json();
      setInfoText(json.explanation || "No details available.");
      setInfoOpen(true);
    } catch {
      setInfoText("Unable to load details right now.");
      setInfoOpen(true);
    }
  };

  useEffect(() => {
    setPage(1);
  }, [market, q, book, onlyPos, minEv, sort, dir, pageSize, includeManual]);

  useEffect(() => {
    setMounted(true);
  }, []);


  return (
    <div className="space-y-6 relative">
      {!IS_PRO && (
        <div className="border rounded-2xl p-4 bg-black/5">
          <div className="font-semibold">Free plan</div>
          <div className="text-sm opacity-70">
            You're viewing the top 10 filtered edges. Upgrade to Pro to see all results, larger pages, and more markets.
          </div>
        </div>
      )}

      {/* Controls */}
      <div className="flex flex-wrap items-center gap-3">
        <div className="flex items-center gap-2">
          <label className="text-sm opacity-70">Market</label>
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={market}
            onChange={(e) => setMarket(e.target.value)}
          >
            {Object.entries(MARKET_OPTIONS).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
        </div>

        <input
          className="border rounded-xl px-3 py-2 text-sm bg-transparent"
          placeholder="Search player..."
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />

        <div className="flex items-center gap-2">
          <label className="text-sm opacity-70">Book</label>
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={book}
            onChange={(e) => setBook(e.target.value)}
          >
            <option value="all">All</option>
            <option value="draftkings">DRAFTKINGS</option>
            <option value="fanduel">FANDUEL</option>
          </select>
        </div>

        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={onlyPos}
            onChange={(e) => setOnlyPos(e.target.checked)}
          />
          Only +EV
        </label>

        <label className="flex items-center gap-2 text-sm">
          <input
            type="checkbox"
            checked={includeManual}
            onChange={(e) => setIncludeManual(e.target.checked)}
          />
          Manual odds
        </label>

        <div className="flex items-center gap-2">
          <label className="text-sm opacity-70">Min EV</label>
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent w-[120px]"
            type="number"
            step="0.01"
            min="0"
            value={minEv}
            onChange={(e) => setMinEv(Number(e.target.value))}
          />
        </div>

        <div className="flex items-center gap-2">
          <label className="text-sm opacity-70">Sort</label>
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={sort}
            onChange={(e) => setSort(e.target.value as any)}
          >
            <option value="ev">EV</option>
            <option value="p">P</option>
          </select>

          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={dir}
            onChange={(e) => setDir(e.target.value as any)}
          >
            <option value="desc">Desc</option>
            <option value="asc">Asc</option>
          </select>
        </div>

        <div className="ml-auto flex items-center gap-2">
          <label className="text-sm opacity-70">Page size</label>
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={pageSize}
            onChange={(e) => IS_PRO && setPageSize(Number(e.target.value))}
            disabled={!IS_PRO}
          >
            {[10, 25, 50, 100].map((n) => (
              <option key={n} value={n}>
                {n}
              </option>
            ))}
          </select>

          <button
            className="border rounded-xl px-3 py-2 text-sm disabled:opacity-40"
            onClick={() => setPage((p) => Math.max(1, p - 1))}
            disabled={safePage <= 1}
          >
            Prev
          </button>

          <span className="text-sm opacity-70">
            Page {safePage} / {totalPages} ({total} rows)
          </span>

          <button
            className="border rounded-xl px-3 py-2 text-sm disabled:opacity-40"
            onClick={() => setPage((p) => Math.min(totalPages, p + 1))}
            disabled={safePage >= totalPages}
          >
            Next
          </button>
        </div>
      </div>

      {/* Table */}
      <div className="border rounded-2xl overflow-auto">
        <table className="w-full table-fixed border-separate border-spacing-0 text-sm min-w-[1360px]">
          <thead>
            <tr className="bg-black/5">
              <th className="px-4 py-3 text-left font-semibold w-[260px]">Player</th>
              <th className="px-4 py-3 text-center font-semibold w-[120px]">Market</th>
              <th className="px-4 py-3 text-center font-semibold w-[90px]">Side</th>
              <th className="px-4 py-3 text-center font-semibold w-[90px]">Line</th>
              <th className="px-4 py-3 text-center font-semibold w-[90px]">Odds</th>
              <th className="px-4 py-3 text-center font-semibold w-[90px]">P</th>
              <th className="px-4 py-3 text-center font-semibold w-[90px]">EV</th>
              <th className="px-4 py-3 text-center font-semibold w-[120px]">Book</th>
              <th className="px-4 py-3 text-center font-semibold w-[120px]">Confidence</th>
              <th className="px-4 py-3 text-center font-semibold w-[70px]">Info</th>
              <th className="px-4 py-3 text-center font-semibold w-[90px]">Log</th>
            </tr>
          </thead>

          <tbody>
            {loading ? (
              <tr>
                <td colSpan={11} className="p-6 text-center opacity-60">
                  Loading...
                </td>
              </tr>
            ) : prettyRows.length === 0 ? (
              <tr>
                <td colSpan={11} className="p-6 text-center opacity-60">
                  No edges found.
                </td>
              </tr>
            ) : (
              prettyRows.map((r) => {
                const c = confidenceBadge(Number(r.p));
                return (
                  <tr key={r.id} className="border-t border-white/10 hover:bg-black/5">
                    <td className="px-4 py-3 text-left whitespace-nowrap overflow-hidden text-ellipsis">
                      {r.player_name}
                    </td>
                    <td className="px-4 py-3 text-center whitespace-nowrap">{r.marketPretty}</td>
                    <td className="px-4 py-3 text-center">{r.sidePretty}</td>
                    <td className="px-4 py-3 text-center tabular-nums">{Number(r.line).toFixed(1)}</td>
                    <td className="px-4 py-3 text-center tabular-nums">{formatDecimalOdds(r)}</td>
                    <td className="px-4 py-3 text-center tabular-nums">{Number(r.p).toFixed(3)}</td>
                    <td className="px-4 py-3 text-center tabular-nums font-semibold">{Number(r.ev).toFixed(3)}</td>
                    <td className="px-4 py-3 text-center whitespace-nowrap overflow-hidden text-ellipsis">
                      {String(r.bookPretty)}
                    </td>
                    <td className="px-4 py-3 text-center">
                      <span className={`inline-flex items-center justify-center px-2 py-1 rounded-lg text-xs font-semibold ${c.cls}`}>
                        {c.text}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-center">
                      <button
                        className="border rounded-full w-7 h-7 text-xs"
                        onClick={() => openInfo(r)}
                        aria-label="Info"
                      >
                        i
                      </button>
                    </td>
                    <td className="px-4 py-3 text-center">
                      <button
                        className="border rounded-lg px-2 py-1 text-xs"
                        onClick={() => logBet(r)}
                      >
                        Log
                      </button>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>

      <p className="text-xs opacity-60">
        P = win probability from the selected Source. EV = expected profit per $1. Confidence is a simple proxy based on how far P is from 0.5.
      </p>
      {mounted &&
        infoOpen &&
        createPortal(
          <div
            className="fixed inset-0 z-[9999] flex items-center justify-center bg-black/50"
            style={{ backdropFilter: "blur(3px)" }}
          >
            <div
              className="relative rounded-3xl border border-white/30 bg-black p-10 text-white shadow-2xl"
              style={{
                width: "88vw",
                maxWidth: "72rem",
                maxHeight: "75vh",
                overflow: "auto",
              }}
            >
              <button
                className="absolute right-3 top-3 text-xs border border-white/60 text-white rounded-full w-7 h-7"
                onClick={() => setInfoOpen(false)}
                aria-label="Close"
              >
                x
              </button>
              <div className="text-base font-semibold text-white">Why this pick</div>
              <p className="mt-4 text-sm leading-relaxed text-white/80">{infoText}</p>
            </div>
          </div>,
          document.body
        )}
    </div>
  );
}
