"use client";

import { useEffect, useMemo, useState } from "react";

type EdgeRow = {
  id?: string;
  player_name: string;
  market: string;
  side: string;
  line: number;
  odds: number;
  p: number;
  ev: number;
  book: string;
  source: string;
  starts_at?: string;
  current?: number | null;
  progress?: number | null;
  minutes?: string | null;
  team_abbr?: string | null;
};

export default function LiveFeed() {
  const [rows, setRows] = useState<EdgeRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [market, setMarket] = useState("player_points");

  useEffect(() => {
    let active = true;

    const load = async () => {
      setLoading(true);
      try {
        const res = await fetch(
          `/api/live-stats?market=${market}&limit=80&lookbackHours=6`
        );
        const json = await res.json();
        if (active) setRows(json.data || []);
      } catch {
        if (active) setRows([]);
      } finally {
        if (active) setLoading(false);
      }
    };

    load();
    const t = setInterval(load, 30000);
    return () => {
      active = false;
      clearInterval(t);
    };
  }, [market]);

  const marketLabel = useMemo(() => {
    if (market === "player_points") return "Points";
    if (market === "player_rebounds") return "Rebounds";
    if (market === "player_assists") return "Assists";
    return market;
  }, [market]);

  return (
    <div className="border rounded-2xl p-4 space-y-4">
      <div className="flex items-center justify-between">
        <div className="text-sm opacity-70">Live props progress (auto-refresh)</div>
        <div className="flex items-center gap-2 text-sm">
          <label className="opacity-70">Market</label>
          <select
            className="border rounded-xl px-2 py-1 text-sm bg-transparent"
            value={market}
            onChange={(e) => setMarket(e.target.value)}
          >
            <option value="player_points">Points</option>
            <option value="player_rebounds">Rebounds</option>
            <option value="player_assists">Assists</option>
          </select>
        </div>
        {loading ? <div className="text-xs opacity-60">Loading...</div> : null}
      </div>

      <div className="space-y-2">
        {rows.length === 0 ? (
          <div className="text-sm opacity-60">No rows yet.</div>
        ) : (
          rows.slice(0, 30).map((r) => (
            <div key={`${r.id}-${r.player_name}-${r.line}`} className="border rounded-xl p-3">
              <div className="flex flex-wrap items-center gap-3 text-sm">
                <span className="font-semibold">{r.player_name}</span>
                <span className="opacity-70">{marketLabel}</span>
                <span className="uppercase">{r.side}</span>
                <span className="tabular-nums">Line {Number(r.line).toFixed(1)}</span>
                <span className="tabular-nums">Now {r.current ?? "-"}</span>
                <span className="tabular-nums">
                  {r.progress !== null && r.progress !== undefined
                    ? `${Math.round(r.progress * 100)}%`
                    : "-"}
                </span>
                <span className="opacity-70">{String(r.book || "").toUpperCase()}</span>
                {r.minutes ? <span className="opacity-70">MIN {r.minutes}</span> : null}
              </div>
              {r.starts_at ? (
                <div className="text-xs opacity-60">Starts: {new Date(r.starts_at).toLocaleString()}</div>
              ) : null}
            </div>
          ))
        )}
      </div>
    </div>
  );
}
