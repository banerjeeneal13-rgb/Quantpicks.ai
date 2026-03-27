"use client";

import { useEffect, useMemo, useState, useCallback } from "react";
import { coerceDecimalOdds } from "@/lib/odds";
import {
  fetchPlayerHeadshot,
  teamLogoUrl,
  nameInitials,
  isTeamMarket,
} from "@/lib/nba-images";
import {
  kellyStake,
  formatStake,
  formatUnits,
  DEFAULT_STAKE_CONFIG,
  type StakeConfig,
} from "@/lib/stake-calculator";

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
  source?: string;
  starts_at?: string;
  team_abbr?: string | null;
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

// ── Player Avatar with headshot ──────────────────────────────────────────────
function PlayerAvatar({
  name,
  market,
  teamAbbr,
  size = 40,
}: {
  name: string;
  market?: string;
  teamAbbr?: string | null;
  size?: number;
}) {
  const [imgUrl, setImgUrl] = useState<string | null>(null);
  const [failed, setFailed] = useState(false);

  const isTeam = isTeamMarket(market || "");

  useEffect(() => {
    if (isTeam && teamAbbr) {
      setImgUrl(teamLogoUrl(teamAbbr));
      return;
    }
    let cancelled = false;
    fetchPlayerHeadshot(name).then((url) => {
      if (!cancelled) setImgUrl(url);
    });
    return () => {
      cancelled = true;
    };
  }, [name, isTeam, teamAbbr]);

  if (imgUrl && !failed) {
    return (
      <img
        src={imgUrl}
        alt={name}
        width={size}
        height={size}
        className="rounded-full object-cover bg-black/10"
        style={{ width: size, height: size }}
        onError={() => setFailed(true)}
      />
    );
  }

  // Fallback: styled initials
  return (
    <div
      className="rounded-full bg-black/10 flex items-center justify-center text-xs font-bold select-none"
      style={{ width: size, height: size }}
    >
      {nameInitials(name)}
    </div>
  );
}

// ── Stake config persisted in localStorage ───────────────────────────────────
function loadStakeConfig(): StakeConfig {
  if (typeof window === "undefined") return DEFAULT_STAKE_CONFIG;
  try {
    const raw = localStorage.getItem("qp_stake_config");
    if (raw) return { ...DEFAULT_STAKE_CONFIG, ...JSON.parse(raw) };
  } catch {}
  return DEFAULT_STAKE_CONFIG;
}

export default function HomeDashboard() {
  const [top3, setTop3] = useState<EdgeRow[]>([]);
  const [premium, setPremium] = useState<EdgeRow[]>([]);
  const [bets, setBets] = useState<BetRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [latestGameDate, setLatestGameDate] = useState<string | null>(null);
  const [picksDate, setPicksDate] = useState<string | null>(null);
  const [stakeConfig] = useState<StakeConfig>(loadStakeConfig);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      try {
        const [dailyRes, betsRes, statsRes] = await Promise.all([
          fetch("/api/daily-picks?type=all"),
          fetch("/api/bets?limit=200"),
          fetch("/api/latest-stats"),
        ]);
        const dailyJson = await dailyRes.json();
        const betsJson = await betsRes.json();
        const statsJson = await statsRes.json();
        if (!cancelled) {
          setTop3(dailyJson.top3 || []);
          setPremium(dailyJson.premium || []);
          setPicksDate(dailyJson.date || null);
          setBets(betsJson.data || []);
          setLatestGameDate(statsJson.latest_game_date || null);
        }
      } catch {
        if (!cancelled) {
          setTop3([]);
          setPremium([]);
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

  const computeStake = useCallback(
    (p: number, odds: number) => kellyStake(p, odds, stakeConfig),
    [stakeConfig]
  );

  // ── Pick card component ──────────────────────────────────────────────────
  function PickCard({ edge, rank }: { edge: EdgeRow; rank?: number }) {
    const odds = coerceDecimalOdds(edge);
    const oddsVal = odds !== null ? odds : Number(edge.odds);
    const stake = computeStake(Number(edge.p), oddsVal);

    return (
      <div className="flex items-center gap-3 border rounded-xl p-3 hover:bg-black/5 transition-colors">
        {rank !== undefined && (
          <div className="text-lg font-bold opacity-40 w-6 text-center">{rank}</div>
        )}
        <PlayerAvatar
          name={edge.player_name}
          market={edge.market}
          teamAbbr={edge.team_abbr}
          size={44}
        />
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap">
            <span className="font-semibold truncate">{edge.player_name}</span>
            <span className="text-xs opacity-60 border rounded-full px-2 py-0.5">
              {marketLabel(edge.market)}
            </span>
          </div>
          <div className="flex flex-wrap gap-2 text-xs mt-1">
            <span className="uppercase font-medium">{edge.side}</span>
            <span className="tabular-nums opacity-70">Line {Number(edge.line).toFixed(1)}</span>
            <span className="tabular-nums opacity-70">
              Odds {oddsVal ? oddsVal.toFixed(2) : "-"}
            </span>
            <span className="tabular-nums">P {Number(edge.p).toFixed(3)}</span>
            <span className="tabular-nums font-semibold text-green-700">
              EV {Number(edge.ev).toFixed(3)}
            </span>
            <span className="uppercase opacity-50">{String(edge.book || "").toUpperCase()}</span>
          </div>
        </div>
        <div className="text-right shrink-0">
          <div className="text-sm font-semibold">{formatStake(stake.stake)}</div>
          <div className="text-xs opacity-50">{formatUnits(stake.units)}</div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-6">
      {/* Stats cards */}
      <div className="grid gap-4 md:grid-cols-4">
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Daily P/L</div>
          <div
            className={`text-2xl font-semibold ${dailyProfit >= 0 ? "text-green-600" : "text-red-600"}`}
          >
            ${dailyProfit.toFixed(2)}
          </div>
        </div>
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Monthly P/L</div>
          <div
            className={`text-2xl font-semibold ${monthlyProfit >= 0 ? "text-green-600" : "text-red-600"}`}
          >
            ${monthlyProfit.toFixed(2)}
          </div>
        </div>
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Premium Picks</div>
          <div className="text-2xl font-semibold">{premium.length}</div>
          <div className="text-xs opacity-50">
            {picksDate ? `Locked ${picksDate}` : ""}
          </div>
        </div>
        <div className="rounded-2xl border p-5">
          <div className="text-sm opacity-70">Latest Game</div>
          <div className="text-2xl font-semibold">{latestGameDate ?? "-"}</div>
        </div>
      </div>

      {/* Top 3 Picks */}
      <div className="border rounded-2xl p-5 space-y-3">
        <div className="flex items-center justify-between">
          <div className="text-sm font-semibold">Top 3 Picks Today</div>
          {picksDate && (
            <div className="text-xs opacity-40">Locked for {picksDate}</div>
          )}
        </div>
        {loading ? (
          <div className="text-sm opacity-60">Loading...</div>
        ) : top3.length === 0 ? (
          <div className="text-sm opacity-60">No picks yet — edges will be snapshotted when available.</div>
        ) : (
          <div className="space-y-2">
            {top3.map((e, idx) => (
              <PickCard key={e.id} edge={e} rank={idx + 1} />
            ))}
          </div>
        )}
      </div>

      {/* Premium Picks (threshold-based, variable count) */}
      <div className="border rounded-2xl p-5 space-y-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <div className="text-sm font-semibold">Premium Picks</div>
            <span className="text-xs bg-amber-500/15 text-amber-700 px-2 py-0.5 rounded-full font-semibold">
              ON RECORD
            </span>
          </div>
          <div className="text-xs opacity-40">
            EV ≥ 0.05 or P ≥ 0.57 · {premium.length} pick{premium.length !== 1 ? "s" : ""} today
          </div>
        </div>
        {loading ? (
          <div className="text-sm opacity-60">Loading...</div>
        ) : premium.length === 0 ? (
          <div className="text-sm opacity-60">
            No picks meet the premium threshold today.
          </div>
        ) : (
          <div className="space-y-2">
            {premium.map((e, idx) => (
              <PickCard key={e.id} edge={e} rank={idx + 1} />
            ))}
          </div>
        )}
      </div>

      <div className="grid gap-6 lg:grid-cols-2">
        {/* Recent Bets */}
        <div className="border rounded-2xl p-5 space-y-3">
          <div className="text-sm font-semibold">Recent Bets</div>
          {recentBets.length === 0 ? (
            <div className="text-sm opacity-60">No bets logged.</div>
          ) : (
            <div className="space-y-2 text-sm">
              {recentBets.map((b) => {
                const odds = coerceDecimalOdds(b);
                const oddsText = odds !== null ? odds.toFixed(2) : "-";
                const impliedText =
                  odds !== null ? impliedProb(odds).toFixed(2) : "-";
                return (
                  <div
                    key={b.id}
                    className="flex flex-wrap gap-2 items-center border rounded-xl p-2"
                  >
                    <PlayerAvatar name={b.player_name} market={b.market} size={32} />
                    <span className="font-semibold">{b.player_name}</span>
                    <span className="opacity-70">{marketLabel(b.market)}</span>
                    <span className="uppercase">{b.side}</span>
                    <span className="tabular-nums">
                      Line {Number(b.line).toFixed(1)}
                    </span>
                    <span className="tabular-nums">Odds {oddsText}</span>
                    <span className="tabular-nums">Implied {impliedText}</span>
                    <span className="tabular-nums font-semibold">
                      Stake ${Number(b.stake || 0).toFixed(2)}
                    </span>
                    <span
                      className={`tabular-nums font-semibold ${Number(b.profit || 0) >= 0 ? "text-green-600" : "text-red-600"}`}
                    >
                      PnL ${Number(b.profit || 0).toFixed(2)}
                    </span>
                  </div>
                );
              })}
            </div>
          )}
        </div>

        {/* Stake Config Summary */}
        <div className="border rounded-2xl p-5 space-y-3">
          <div className="text-sm font-semibold">Stake Calculator</div>
          <div className="grid grid-cols-2 gap-3 text-sm">
            <div className="border rounded-xl p-3">
              <div className="text-xs opacity-50">Bankroll</div>
              <div className="text-lg font-semibold">
                ${stakeConfig.bankroll.toFixed(0)}
              </div>
            </div>
            <div className="border rounded-xl p-3">
              <div className="text-xs opacity-50">Unit Size</div>
              <div className="text-lg font-semibold">
                ${((stakeConfig.bankroll * stakeConfig.unitPct) / 100).toFixed(2)}
              </div>
            </div>
            <div className="border rounded-xl p-3">
              <div className="text-xs opacity-50">Max Bet</div>
              <div className="text-lg font-semibold">
                ${((stakeConfig.bankroll * stakeConfig.maxPct) / 100).toFixed(2)}
              </div>
            </div>
            <div className="border rounded-xl p-3">
              <div className="text-xs opacity-50">Method</div>
              <div className="text-lg font-semibold">
                {stakeConfig.kellyFraction === 0.25
                  ? "¼ Kelly"
                  : stakeConfig.kellyFraction === 0.5
                    ? "½ Kelly"
                    : `${((stakeConfig.kellyFraction || 0.25) * 100).toFixed(0)}% Kelly`}
              </div>
            </div>
          </div>
          <p className="text-xs opacity-40">
            Configure in Bankroll Manager. Stakes shown next to each pick use
            quarter-Kelly criterion with your bankroll settings.
          </p>
        </div>
      </div>
    </div>
  );
}
