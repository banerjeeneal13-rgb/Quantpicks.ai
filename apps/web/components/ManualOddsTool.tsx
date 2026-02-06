"use client";

import { useEffect, useMemo, useState } from "react";

const MARKET_OPTIONS: Record<string, string> = {
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

function toLocalInputValue(d: Date) {
  const tzOffset = d.getTimezoneOffset() * 60000;
  return new Date(d.getTime() - tzOffset).toISOString().slice(0, 16);
}

type ManualOddsRow = {
  id?: string;
  pulled_at?: string | null;
  game_date?: string | null;
  game?: string | null;
  event_id?: string | null;
  player_name: string;
  market: string;
  line: number;
  over_odds?: number | null;
  under_odds?: number | null;
  book: string;
  notes?: string | null;
};

export default function ManualOddsTool() {
  const [rows, setRows] = useState<ManualOddsRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [marketFilter, setMarketFilter] = useState("player_points");
  const [q, setQ] = useState("");
  const [bookFilter, setBookFilter] = useState("");

  const [form, setForm] = useState({
    player_name: "",
    market: "player_points",
    line: "",
    over_odds: "",
    under_odds: "",
    book: "",
    game: "",
    game_date: "",
    pulled_at: toLocalInputValue(new Date()),
    notes: "",
  });

  const fetchRows = () => {
    setLoading(true);
    const params = new URLSearchParams();
    params.set("limit", "200");
    if (marketFilter) params.set("market", marketFilter);
    if (q.trim()) params.set("q", q.trim());
    if (bookFilter.trim()) params.set("book", bookFilter.trim().toLowerCase());
    fetch(`/api/manual-odds?${params.toString()}`)
      .then((r) => r.json())
      .then((json) => setRows(json.data || []))
      .catch(() => setRows([]))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    fetchRows();
  }, [marketFilter, q, bookFilter]);

  const submitManual = async () => {
    const payload = {
      player_name: form.player_name,
      market: form.market,
      line: Number(form.line),
      over_odds: form.over_odds ? Number(form.over_odds) : null,
      under_odds: form.under_odds ? Number(form.under_odds) : null,
      book: form.book,
      game: form.game || null,
      game_date: form.game_date || null,
      pulled_at: form.pulled_at ? new Date(form.pulled_at).toISOString() : null,
      notes: form.notes || null,
    };

    if (!payload.player_name || !payload.market || !payload.book) return;
    if (!Number.isFinite(payload.line)) return;
    if (payload.over_odds === null && payload.under_odds === null) return;

    const res = await fetch("/api/manual-odds", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    if (json?.data) {
      setRows([json.data, ...rows]);
      setForm({
        player_name: "",
        market: "player_points",
        line: "",
        over_odds: "",
        under_odds: "",
        book: "",
        game: "",
        game_date: "",
        pulled_at: toLocalInputValue(new Date()),
        notes: "",
      });
    }
  };

  const summary = useMemo(() => {
    const map = new Map<
      string,
      {
        player_name: string;
        market: string;
        lines: number[];
        books: string[];
        over: number[];
        under: number[];
      }
    >();

    rows.forEach((r) => {
      const key = `${r.player_name}||${r.market}`;
      if (!map.has(key)) {
        map.set(key, {
          player_name: r.player_name,
          market: r.market,
          lines: [],
          books: [],
          over: [],
          under: [],
        });
      }
      const entry = map.get(key)!;
      entry.lines.push(Number(r.line));
      entry.books.push(String(r.book || "").toUpperCase());
      if (r.over_odds !== null && r.over_odds !== undefined) entry.over.push(Number(r.over_odds));
      if (r.under_odds !== null && r.under_odds !== undefined) entry.under.push(Number(r.under_odds));
    });

    return Array.from(map.values()).map((entry) => {
      const minLine = Math.min(...entry.lines);
      const maxLine = Math.max(...entry.lines);
      const bookList = Array.from(new Set(entry.books)).join(", ");
      const minOver = entry.over.length ? Math.min(...entry.over) : null;
      const maxOver = entry.over.length ? Math.max(...entry.over) : null;
      const minUnder = entry.under.length ? Math.min(...entry.under) : null;
      const maxUnder = entry.under.length ? Math.max(...entry.under) : null;
      return {
        player_name: entry.player_name,
        market: entry.market,
        lineRange: minLine === maxLine ? `${minLine}` : `${minLine} - ${maxLine}`,
        books: bookList,
        overRange: minOver === null ? "-" : minOver === maxOver ? `${minOver}` : `${minOver} - ${maxOver}`,
        underRange: minUnder === null ? "-" : minUnder === maxUnder ? `${minUnder}` : `${minUnder} - ${maxUnder}`,
      };
    });
  }, [rows]);

  return (
    <div className="space-y-6">
      <div className="border rounded-2xl p-6 space-y-4">
        <div className="text-sm font-semibold">Manual odds entry</div>
        <div className="grid gap-3 md:grid-cols-3">
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Player name"
            value={form.player_name}
            onChange={(e) => setForm({ ...form, player_name: e.target.value })}
          />
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={form.market}
            onChange={(e) => setForm({ ...form, market: e.target.value })}
          >
            {Object.entries(MARKET_OPTIONS).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Line"
            value={form.line}
            onChange={(e) => setForm({ ...form, line: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Over odds"
            value={form.over_odds}
            onChange={(e) => setForm({ ...form, over_odds: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Under odds"
            value={form.under_odds}
            onChange={(e) => setForm({ ...form, under_odds: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Book (draftkings, fanduel...)"
            value={form.book}
            onChange={(e) => setForm({ ...form, book: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Game (optional)"
            value={form.game}
            onChange={(e) => setForm({ ...form, game: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            type="date"
            value={form.game_date}
            onChange={(e) => setForm({ ...form, game_date: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            type="datetime-local"
            value={form.pulled_at}
            onChange={(e) => setForm({ ...form, pulled_at: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Notes (optional)"
            value={form.notes}
            onChange={(e) => setForm({ ...form, notes: e.target.value })}
          />
        </div>
        <button className="border rounded-xl px-3 py-2 text-sm" onClick={submitManual}>
          Save line
        </button>
      </div>

      <div className="border rounded-2xl p-6 space-y-3">
        <div className="flex flex-wrap items-center gap-3">
          <div className="text-sm font-semibold">Line comparison</div>
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={marketFilter}
            onChange={(e) => setMarketFilter(e.target.value)}
          >
            {Object.entries(MARKET_OPTIONS).map(([value, label]) => (
              <option key={value} value={value}>
                {label}
              </option>
            ))}
          </select>
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Search player..."
            value={q}
            onChange={(e) => setQ(e.target.value)}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Book filter..."
            value={bookFilter}
            onChange={(e) => setBookFilter(e.target.value)}
          />
          <button className="border rounded-xl px-3 py-2 text-sm" onClick={fetchRows}>
            Refresh
          </button>
        </div>

        {loading ? (
          <div className="text-sm opacity-60">Loading...</div>
        ) : summary.length === 0 ? (
          <div className="text-sm opacity-60">No manual lines yet.</div>
        ) : (
          <div className="border rounded-xl overflow-auto">
            <table className="w-full table-fixed text-sm min-w-[920px]">
              <thead className="bg-black/5">
                <tr>
                  <th className="px-4 py-3 text-left w-[240px]">Player</th>
                  <th className="px-4 py-3 text-left w-[140px]">Market</th>
                  <th className="px-4 py-3 text-left w-[140px]">Line Range</th>
                  <th className="px-4 py-3 text-left w-[160px]">Over Odds</th>
                  <th className="px-4 py-3 text-left w-[160px]">Under Odds</th>
                  <th className="px-4 py-3 text-left w-[260px]">Books</th>
                </tr>
              </thead>
              <tbody>
                {summary.map((s) => (
                  <tr key={`${s.player_name}-${s.market}`} className="border-t border-white/10">
                    <td className="px-4 py-3">{s.player_name}</td>
                    <td className="px-4 py-3">{marketLabel(s.market)}</td>
                    <td className="px-4 py-3 tabular-nums">{s.lineRange}</td>
                    <td className="px-4 py-3 tabular-nums">{s.overRange}</td>
                    <td className="px-4 py-3 tabular-nums">{s.underRange}</td>
                    <td className="px-4 py-3">{s.books}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      <div className="border rounded-2xl p-6 space-y-3">
        <div className="text-sm font-semibold">Recent manual lines</div>
        {rows.length === 0 ? (
          <div className="text-sm opacity-60">No manual lines yet.</div>
        ) : (
          <div className="space-y-2">
            {rows.slice(0, 20).map((r) => (
              <div key={r.id} className="border rounded-xl p-3 text-sm">
                <div className="flex flex-wrap gap-3 items-center">
                  <span className="font-semibold">{r.player_name}</span>
                  <span className="opacity-70">{marketLabel(r.market)}</span>
                  <span className="tabular-nums">Line {Number(r.line).toFixed(1)}</span>
                  {r.over_odds !== null && r.over_odds !== undefined && (
                    <span className="tabular-nums">O {Number(r.over_odds).toFixed(2)}</span>
                  )}
                  {r.under_odds !== null && r.under_odds !== undefined && (
                    <span className="tabular-nums">U {Number(r.under_odds).toFixed(2)}</span>
                  )}
                  <span className="uppercase">{String(r.book || "")}</span>
                  {r.pulled_at && (
                    <span className="opacity-60">
                      {new Date(r.pulled_at).toLocaleString()}
                    </span>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
