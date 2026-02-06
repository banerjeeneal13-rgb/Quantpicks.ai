"use client";

import { useEffect, useMemo, useState } from "react";

type BetRow = {
  id?: string;
  created_at?: string;
  starts_at?: string | null;
  player_name: string;
  market: string;
  side: string;
  line: number;
  odds: number;
  stake: number;
  unit?: number | null;
  result?: string | null;
  profit?: number | null;
  book?: string | null;
  source?: string | null;
};

function computeProfit(row: BetRow) {
  if (row.profit !== null && row.profit !== undefined) return row.profit;
  const result = String(row.result || "").toLowerCase();
  if (!result) return 0;
  if (result === "win") return row.stake * (row.odds - 1);
  if (result === "lose") return -row.stake;
  return 0;
}

export default function BankrollTool() {
  const [startingBankroll, setStartingBankroll] = useState(1000);
  const [bankroll, setBankroll] = useState(1000);
  const [unitPct, setUnitPct] = useState(1.0);
  const [maxPct, setMaxPct] = useState(3.0);
  const [bets, setBets] = useState<BetRow[]>([]);

  const [form, setForm] = useState({
    player_name: "",
    market: "player_points",
    side: "over",
    line: "",
    odds: "",
    stake: "",
    book: "",
    notes: "",
  });

  const unit = useMemo(() => (bankroll * unitPct) / 100, [bankroll, unitPct]);
  const maxBet = useMemo(() => (bankroll * maxPct) / 100, [bankroll, maxPct]);

  useEffect(() => {
    fetch("/api/bets?limit=500")
      .then((r) => r.json())
      .then((json) => setBets(json.data || []))
      .catch(() => setBets([]));
  }, []);

  const totalProfit = useMemo(() => bets.reduce((s, b) => s + computeProfit(b), 0), [bets]);
  const currentBankroll = startingBankroll + totalProfit;

  const todayStr = new Date().toISOString().slice(0, 10);
  const unitsToday = useMemo(() => {
    if (unit <= 0) return 0;
    return bets
      .filter((b) => (b.created_at || "").slice(0, 10) === todayStr)
      .reduce((s, b) => s + b.stake / unit, 0);
  }, [bets, unit, todayStr]);

  const [selectedDay, setSelectedDay] = useState<string | null>(null);

  const dailyMap = useMemo(() => {
    const map: Record<string, number> = {};
    bets.forEach((b) => {
      const day = String(b.created_at || "").slice(0, 10);
      if (!day) return;
      map[day] = (map[day] || 0) + computeProfit(b);
    });
    return map;
  }, [bets]);

  const monthKey = new Date().toISOString().slice(0, 7);
  const monthDays = useMemo(() => {
    const now = new Date();
    const year = now.getFullYear();
    const month = now.getMonth();
    const first = new Date(year, month, 1);
    const last = new Date(year, month + 1, 0);
    const days: { date: string; value: number }[] = [];
    for (let d = 1; d <= last.getDate(); d += 1) {
      const dayStr = new Date(year, month, d).toISOString().slice(0, 10);
      days.push({ date: dayStr, value: dailyMap[dayStr] || 0 });
    }
    return { first, days };
  }, [dailyMap, monthKey]);

  const selectedBets = useMemo(() => {
    if (!selectedDay) return [];
    return bets.filter((b) => String(b.created_at || "").slice(0, 10) === selectedDay);
  }, [bets, selectedDay]);

  const bankrollSeries = useMemo(() => {
    const entries = Object.keys(dailyMap)
      .sort()
      .map((d) => ({ date: d, value: dailyMap[d] || 0 }));
    let running = startingBankroll;
    return entries.map((e) => {
      running += e.value;
      return { date: e.date, value: running };
    });
  }, [dailyMap, startingBankroll]);

  const recommendations = useMemo(() => {
    const recs = [];
    const drawdown = startingBankroll > 0 ? (currentBankroll - startingBankroll) / startingBankroll : 0;
    if (unitsToday >= 6) {
      recs.push("High daily exposure. Consider pausing or reducing unit size.");
    } else if (unitsToday >= 4) {
      recs.push("Moderate exposure today. Stick to your top edges only.");
    }
    if (drawdown <= -0.1) {
      recs.push("Drawdown >10%. Cut unit size by 25-50% until recovery.");
    }
    if (drawdown >= 0.1) {
      recs.push("Up >10%. Keep size steady; avoid scaling too quickly.");
    }
    if (recs.length === 0) {
      recs.push("Bankroll is stable. Maintain current unit size.");
    }
    return recs;
  }, [unitsToday, startingBankroll, currentBankroll]);

  const submitManual = async () => {
    const payload = {
      ...form,
      line: Number(form.line),
      odds: Number(form.odds),
      stake: Number(form.stake),
      unit,
      source: "manual",
    };
    if (!payload.player_name || !payload.market || !payload.side) return;
    if (!Number.isFinite(payload.line) || !Number.isFinite(payload.odds) || !Number.isFinite(payload.stake)) return;

    const res = await fetch("/api/bets", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    if (json?.data) {
      setBets([json.data, ...bets]);
      setForm({
        player_name: "",
        market: "player_points",
        side: "over",
        line: "",
        odds: "",
        stake: "",
        book: "",
        notes: "",
      });
    }
  };

  const statusForBet = (bet: BetRow) => {
    if (bet.result) return bet.result.toUpperCase();
    if (!bet.starts_at) return "PENDING";
    const startsAt = new Date(bet.starts_at).getTime();
    if (Number.isNaN(startsAt)) return "PENDING";
    return startsAt > Date.now() ? "UPCOMING" : "LIVE";
  };

  return (
    <div className="space-y-6">
      <div className="border rounded-2xl p-6 space-y-4">
        <div className="grid gap-4 md:grid-cols-3">
          <label className="space-y-1">
            <div className="text-sm opacity-70">Starting bankroll</div>
            <input
              className="w-full border rounded-xl px-3 py-2 text-sm bg-transparent"
              type="number"
              min="0"
              value={startingBankroll}
              onChange={(e) => setStartingBankroll(Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm opacity-70">Current bankroll</div>
            <input
              className="w-full border rounded-xl px-3 py-2 text-sm bg-transparent"
              type="number"
              min="0"
              value={bankroll}
              onChange={(e) => setBankroll(Number(e.target.value))}
            />
          </label>

          <div className="rounded-xl border p-4">
            <div className="text-sm opacity-70">Bankroll (auto)</div>
            <div className="text-2xl font-semibold">${currentBankroll.toFixed(2)}</div>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-3">
          <label className="space-y-1">
            <div className="text-sm opacity-70">Unit size (%)</div>
            <input
              className="w-full border rounded-xl px-3 py-2 text-sm bg-transparent"
              type="number"
              min="0.1"
              step="0.1"
              value={unitPct}
              onChange={(e) => setUnitPct(Number(e.target.value))}
            />
          </label>

          <label className="space-y-1">
            <div className="text-sm opacity-70">Max bet (%)</div>
            <input
              className="w-full border rounded-xl px-3 py-2 text-sm bg-transparent"
              type="number"
              min="0.5"
              step="0.5"
              value={maxPct}
              onChange={(e) => setMaxPct(Number(e.target.value))}
            />
          </label>

          <div className="rounded-xl border p-4">
            <div className="text-sm opacity-70">Units today</div>
            <div className="text-2xl font-semibold">{unitsToday.toFixed(2)}</div>
          </div>
        </div>

        <div className="grid gap-4 md:grid-cols-2">
          <div className="rounded-xl border p-4">
            <div className="text-sm opacity-70">Unit size</div>
            <div className="text-2xl font-semibold">${unit.toFixed(2)}</div>
          </div>
          <div className="rounded-xl border p-4">
            <div className="text-sm opacity-70">Max bet</div>
            <div className="text-2xl font-semibold">${maxBet.toFixed(2)}</div>
          </div>
        </div>
      </div>

      <div className="border rounded-2xl p-6 space-y-2">
        <div className="text-sm font-semibold">Strategy notes</div>
        {recommendations.map((r, idx) => (
          <div key={idx} className="text-sm opacity-70">
            {r}
          </div>
        ))}
      </div>

      <div className="border rounded-2xl p-6 space-y-4">
        <div className="text-sm font-semibold">Manual bet log</div>
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
            <option value="player_points">Points</option>
            <option value="player_rebounds">Rebounds</option>
            <option value="player_assists">Assists</option>
          </select>
          <select
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            value={form.side}
            onChange={(e) => setForm({ ...form, side: e.target.value })}
          >
            <option value="over">Over</option>
            <option value="under">Under</option>
          </select>
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Line"
            value={form.line}
            onChange={(e) => setForm({ ...form, line: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Odds (decimal)"
            value={form.odds}
            onChange={(e) => setForm({ ...form, odds: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Stake"
            value={form.stake}
            onChange={(e) => setForm({ ...form, stake: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Book (optional)"
            value={form.book}
            onChange={(e) => setForm({ ...form, book: e.target.value })}
          />
          <input
            className="border rounded-xl px-3 py-2 text-sm bg-transparent"
            placeholder="Notes (optional)"
            value={form.notes}
            onChange={(e) => setForm({ ...form, notes: e.target.value })}
          />
        </div>
        <button className="border rounded-xl px-3 py-2 text-sm" onClick={submitManual}>
          Log bet
        </button>
      </div>

      <div className="border rounded-2xl p-6 space-y-3">
        <div className="text-sm font-semibold">Recent bets</div>
        {bets.length === 0 ? (
          <div className="text-sm opacity-60">No bets logged yet.</div>
        ) : (
          <div className="space-y-2">
            {bets.slice(0, 20).map((b) => (
              <div key={b.id} className="border rounded-xl p-3 text-sm">
                <div className="flex flex-wrap gap-3 items-center">
                  <span className="font-semibold">{b.player_name}</span>
                  <span className="opacity-70">{b.market}</span>
                  <span className="uppercase">{b.side}</span>
                  <span className="tabular-nums">Line {Number(b.line).toFixed(1)}</span>
                  <span className="tabular-nums">Odds {Number(b.odds).toFixed(2)}</span>
                  <span className="tabular-nums">Stake {Number(b.stake).toFixed(2)}</span>
                  <span className="tabular-nums font-semibold">
                    PnL {computeProfit(b).toFixed(2)}
                  </span>
                  <span className="border rounded-lg px-2 py-1 text-xs uppercase">
                    {statusForBet(b)}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="border rounded-2xl p-6 space-y-4">
        <div className="text-sm font-semibold">Monthly calendar</div>
        <div className="grid grid-cols-7 gap-2 text-xs">
          {monthDays.days.map((d) => {
            const color =
              d.value > 0 ? "bg-green-600/15 text-green-700" : d.value < 0 ? "bg-red-600/15 text-red-700" : "bg-black/5";
            return (
              <button
                key={d.date}
                className={`rounded-lg p-2 text-left ${color} ${selectedDay === d.date ? "ring-2 ring-black/30" : ""}`}
                onClick={() => setSelectedDay(d.date)}
              >
                <div className="opacity-70">{d.date.slice(-2)}</div>
                <div className="tabular-nums font-semibold">{d.value.toFixed(2)}</div>
              </button>
            );
          })}
        </div>
        {selectedDay && (
          <div className="space-y-2 text-sm">
            <div className="font-semibold">Bets on {selectedDay}</div>
            {selectedBets.length === 0 ? (
              <div className="opacity-60">No bets logged.</div>
            ) : (
              selectedBets.map((b) => (
                <div key={b.id} className="border rounded-lg p-2">
                  <div className="flex flex-wrap gap-2 items-center">
                    <span className="font-semibold">{b.player_name}</span>
                    <span className="opacity-70">{b.market}</span>
                    <span className="uppercase">{b.side}</span>
                    <span className="tabular-nums">Line {Number(b.line).toFixed(1)}</span>
                    <span className="tabular-nums">Odds {Number(b.odds).toFixed(2)}</span>
                    <span className="tabular-nums font-semibold">PnL {computeProfit(b).toFixed(2)}</span>
                    {String(b.result || "").toLowerCase() === "lose" && (
                      <span className="text-xs opacity-70">
                        Implied win prob { (100 / Number(b.odds || 1)).toFixed(1) }%
                      </span>
                    )}
                  </div>
                </div>
              ))
            )}
          </div>
        )}
      </div>

      <div className="border rounded-2xl p-6 space-y-4">
        <div className="text-sm font-semibold">Bankroll progression</div>
        {bankrollSeries.length === 0 ? (
          <div className="text-sm opacity-60">No history yet.</div>
        ) : (
          <svg viewBox="0 0 300 120" className="w-full h-32">
            <polyline
              fill="none"
              stroke="#16a34a"
              strokeWidth="2"
              points={bankrollSeries
                .map((p, idx) => {
                  const x = (idx / Math.max(1, bankrollSeries.length - 1)) * 300;
                  const min = Math.min(...bankrollSeries.map((v) => v.value));
                  const max = Math.max(...bankrollSeries.map((v) => v.value));
                  const y = 110 - ((p.value - min) / Math.max(1, max - min)) * 100;
                  return `${x},${y}`;
                })
                .join(" ")}
            />
          </svg>
        )}
      </div>
    </div>
  );
}
