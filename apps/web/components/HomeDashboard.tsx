"use client";

import { useEffect, useMemo, useState } from "react";
import { coerceDecimalOdds } from "@/lib/odds";

type EdgeRow = {
  id: string;
  player_name: string;
  market: string;
  side: string;
  line: number;
  odds: number;
  p: number;
  ev: number;
  book: string;
};

type BetRow = {
  id?: string;
  created_at?: string;
  player_name: string;
  market: string;
  side: string;
  line: number;
  odds: number;
  stake: number;
  result?: string | null;
  profit?: number | null;
};

function marketLabel(market: string) {
  const map: Record<string, string> = {
    moneyline: "Moneyline",
    spread: "Spread",
    total: "Total",
    player_points: "Points",
    player_rebounds: "Rebounds",
    player_assists: "Assists",
    player_threes: "3PT",
    player_points_rebounds_assists: "PRA",
    player_points_rebounds: "PR",
    player_points_assists: "PA",
    player_rebounds_assists: "RA",
    player_blocks_steals: "Stocks",
  };
  return map[market] ?? market;
}

function impliedProb(decimalOdds: number) {
  if (!decimalOdds || decimalOdds <= 1) return 0;
  return 1 / decimalOdds;
}

export default function HomeDashboard() {
  const [edges, setEdges] = useState<EdgeRow[]>([]);
  const [bets, setBets] = useState<BetRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [latestGameDate, setLatestGameDate] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const edgesRes = await fetch("/api/top-edges?market=all&pageSize=5&onlyPos=1");
        const edgesJson = await edgesRes.json();
        const betsRes = await fetch("/api/bets?limit=200");
        const betsJson = await betsRes.json();
        const statsRes = await fetch("/api/latest-stats");
        const statsJson = await statsRes.json();
        if (!cancelled) {
          setEdges(edgesJson.data || []);
          setBets(betsJson.data || []);
          setLatestGameDate(statsJson.latest_game_date || null);
        }
      } catch {
        if (!cancelled) {
          setEdges([]);
          setBets([]);
          setLatestGameDate(null);
        }
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, []);

  const today = new Date().toISOString().slice(0, 10);
  const month = new Date().toISOString().slice(0, 7);

  const dailyProfit = useMemo(() => {
    return bets
      .filter((b) => (b.created_at || "").slice(0, 10) === today)
      .reduce((sum, b) => sum + Number(b.profit || 0), 0);
  }, [bets, today]);

  const monthlyProfit = useMemo(() => {
    return bets
      .filter((b) => (b.created_at || "").slice(0, 7) === month)
      .reduce((sum, b) => sum + Number(b.profit || 0), 0);
  }, [bets, month]);

  const recentBets = bets.slice(0, 5);

  return (
    <div className="space-y-6">
      <div className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Daily P/L</div>
          <div className={`text-2xl font-semibold ${dailyProfit >= 0 ? "text-green-600" : "text-red-600"}`}>
            {dailyProfit.toFixed(2)}
          </div>
        </div>
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Monthly P/L</div>
          <div className={`text-2xl font-semibold ${monthlyProfit >= 0 ? "text-green-600" : "text-red-600"}`}>
            {monthlyProfit.toFixed(2)}
          </div>
        </div>
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Top 5 Picks</div>
          <div className="text-2xl font-semibold">{edges.length}</div>
        </div>
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Latest Game</div>
          <div className="text-2xl font-semibold">
            {latestGameDate ?? "-"}
          </div>
        </div>
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        <div className="border rounded-2xl p-5 space-y-3">
          <div className="text-sm font-semibold">Top 5 Picks Today</div>
          {loading ? (
            <div className="text-sm opacity-60">Loading...</div>
          ) : edges.length === 0 ? (
            <div className="text-sm opacity-60">No picks yet.</div>
          ) : (
            <div className="space-y-2 text-sm">
              {edges.map((e) => {
                const odds = coerceDecimalOdds(e);
                const oddsText = odds !== null ? odds.toFixed(2) : "-";
                return (
                  <div key={e.id} className="flex flex-wrap gap-2 items-center">
                    <span className="font-semibold">{e.player_name}</span>
                    <span className="opacity-70">{marketLabel(e.market)}</span>
                    <span className="uppercase">{e.side}</span>
                    <span className="tabular-nums">Line {Number(e.line).toFixed(1)}</span>
                    <span className="tabular-nums">Odds {oddsText}</span>
                    <span className="tabular-nums">P {Number(e.p).toFixed(2)}</span>
                    <span className="tabular-nums font-semibold">EV {Number(e.ev).toFixed(2)}</span>
                    <span className="uppercase">{String(e.book || "").toUpperCase()}</span>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        <div className="border rounded-2xl p-5 space-y-3">
          <div className="text-sm font-semibold">Recent Bets</div>
          {recentBets.length === 0 ? (
            <div className="text-sm opacity-60">No bets logged.</div>
          ) : (
            <div className="space-y-2 text-sm">
              {recentBets.map((b) => {
                const odds = coerceDecimalOdds(b);
                const oddsText = odds !== null ? odds.toFixed(2) : "-";
                const impliedText = odds !== null ? impliedProb(odds).toFixed(2) : "-";
                return (
                  <div key={b.id} className="flex flex-wrap gap-2 items-center">
                    <span className="font-semibold">{b.player_name}</span>
                    <span className="opacity-70">{marketLabel(b.market)}</span>
                    <span className="uppercase">{b.side}</span>
                    <span className="tabular-nums">Line {Number(b.line).toFixed(1)}</span>
                    <span className="tabular-nums">Odds {oddsText}</span>
                    <span className="tabular-nums">Implied {impliedText}</span>
                    <span className="tabular-nums font-semibold">PnL {Number(b.profit || 0).toFixed(2)}</span>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
