import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { coerceDecimalOdds } from "@/lib/odds";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

function impliedProbFromDecimal(decimalOdds: number) {
  if (!decimalOdds || decimalOdds <= 1) return null;
  return 1 / decimalOdds;
}

function noVigPair(overOdds: number, underOdds: number) {
  const po = impliedProbFromDecimal(overOdds);
  const pu = impliedProbFromDecimal(underOdds);
  if (po === null || pu === null) return null;
  const s = po + pu;
  if (s <= 0) return null;
  return [po / s, pu / s];
}

function evPerDollar(p: number, decimalOdds: number) {
  if (!Number.isFinite(p) || !decimalOdds || decimalOdds <= 1) return null;
  return p * decimalOdds - 1;
}

function supabaseAdmin() {
  if (!SUPABASE_URL) throw new Error("Missing SUPABASE_URL");
  if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
}

type ManualOddsRow = {
  id?: string;
  created_at?: string | null;
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

function buildManualEdges(rows: ManualOddsRow[]) {
  const out: any[] = [];
  for (const row of rows) {
    const line = Number(row.line);
    if (!Number.isFinite(line)) continue;
    const overOdds = coerceDecimalOdds({ odds: row.over_odds });
    const underOdds = coerceDecimalOdds({ odds: row.under_odds });
    const pair = overOdds !== null && underOdds !== null ? noVigPair(overOdds, underOdds) : null;
    const createdAt = row.pulled_at || row.created_at || null;

    if (overOdds !== null) {
      const pOver = pair ? pair[0] : impliedProbFromDecimal(overOdds);
      if (pOver !== null) {
        const ev = evPerDollar(pOver, overOdds);
        if (ev !== null) {
          out.push({
            id: row.id || `manual-${row.player_name}-${row.market}-${line}-over-${row.book}-${createdAt}`,
            provider: "manual",
            event_id: row.event_id || null,
            sport: "NBA",
            market: row.market,
            player_name: row.player_name,
            side: "over",
            line,
            book: row.book,
            odds: overOdds,
            p: pOver,
            ev,
            starts_at: null,
            source: "manual",
            created_at: createdAt,
          });
        }
      }
    }

    if (underOdds !== null) {
      const pUnder = pair ? pair[1] : impliedProbFromDecimal(underOdds);
      if (pUnder !== null) {
        const ev = evPerDollar(pUnder, underOdds);
        if (ev !== null) {
          out.push({
            id: row.id || `manual-${row.player_name}-${row.market}-${line}-under-${row.book}-${createdAt}`,
            provider: "manual",
            event_id: row.event_id || null,
            sport: "NBA",
            market: row.market,
            player_name: row.player_name,
            side: "under",
            line,
            book: row.book,
            odds: underOdds,
            p: pUnder,
            ev,
            starts_at: null,
            source: "manual",
            created_at: createdAt,
          });
        }
      }
    }
  }
  return out;
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);

    const market = searchParams.get("market") || "player_points";
    const page = Math.max(Number(searchParams.get("page") || "1"), 1);
    const pageSize = Math.min(Math.max(Number(searchParams.get("pageSize") || "50"), 1), 300);
    const includeManual = (searchParams.get("includeManual") || "1") !== "0";

    const sort = searchParams.get("sort") || "ev";
    const dir = (searchParams.get("dir") || "desc").toLowerCase() === "asc";

    const onlyPos = (searchParams.get("onlyPos") || "1") !== "0";
    const evMinRaw = searchParams.get("evMin") ?? searchParams.get("minEv");
    let evMin = Number(evMinRaw ?? "");
    if (!Number.isFinite(evMin)) {
      evMin = onlyPos ? 0 : Number.NaN;
    }

    const book = (searchParams.get("book") || "").toLowerCase().trim();
    const q = (searchParams.get("q") || "").toLowerCase().trim();
    const source = (searchParams.get("source") || "").toLowerCase().trim();

    // Only show edges for games that haven't started yet.
    // Default: include games through end of tomorrow local time (fallback to 72h if empty).
    const freshHoursParam = searchParams.get("freshHours");
    const now = new Date();
    const defaultFreshHours = (() => {
      const endTomorrow = new Date(now);
      endTomorrow.setDate(endTomorrow.getDate() + 1);
      endTomorrow.setHours(23, 59, 59, 999);
      const hours = Math.max(1, Math.ceil((endTomorrow.getTime() - now.getTime()) / 36e5));
      return Math.min(hours, 168);
    })();
    const freshHours = Math.max(
      1,
      Math.min(Number(freshHoursParam || defaultFreshHours), 168)
    );
    const fallbackHours = Math.max(
      freshHours,
      Math.min(Number(searchParams.get("fallbackHours") || "72"), 168)
    );
    const maxAgeHours = Math.max(1, Math.min(Number(searchParams.get("maxAgeHours") || "168"), 168));

    const maxStart = new Date(now.getTime() + freshHours * 60 * 60 * 1000);
    const minCreated = new Date(now.getTime() - maxAgeHours * 60 * 60 * 1000);

    const sb = supabaseAdmin();

    const sortColumn = ["ev", "p", "created_at"].includes(sort) ? sort : "ev";
    const rangeStart = (page - 1) * pageSize;
    const rangeEnd = rangeStart + pageSize - 1;

    const buildQuery = (maxIso: string) => {
      let query = sb
        .from("edges")
        .select("*")
        .gte("created_at", minCreated.toISOString())
        .gte("starts_at", now.toISOString())
        .lte("starts_at", maxIso)
        .order(sortColumn, { ascending: dir });
      if (market !== "all") {
        query = query.eq("market", market);
      }
      if (onlyPos || Number.isFinite(evMin)) {
        query = query.gte("ev", evMin);
      }
      if (book) query = query.eq("book", book);
      if (q) query = query.ilike("player_name", `%${q}%`);
      if (source) {
        if (source === "model") {
          query = query.ilike("source", "points_model%");
        } else {
          query = query.eq("source", source);
        }
      }
      return query;
    };

    const buildStaleQuery = () => {
      let query = sb
        .from("edges")
        .select("*")
        .gte("created_at", minCreated.toISOString())
        .order(sortColumn, { ascending: dir });
      if (market !== "all") {
        query = query.eq("market", market);
      }
      if (onlyPos || Number.isFinite(evMin)) {
        query = query.gte("ev", evMin);
      }
      if (book) query = query.eq("book", book);
      if (q) query = query.ilike("player_name", `%${q}%`);
      if (source) {
        if (source === "model") {
          query = query.ilike("source", "points_model%");
        } else {
          query = query.eq("source", source);
        }
      }
      return query;
    };

    let { data, error } = await buildQuery(maxStart.toISOString());
    let edgesRows = data ?? [];
    if (!edgesRows.length && fallbackHours > freshHours) {
      const maxStartFallback = new Date(now.getTime() + fallbackHours * 60 * 60 * 1000);
      const res2 = await buildQuery(maxStartFallback.toISOString());
      if (res2.error && !error) error = res2.error;
      edgesRows = res2.data ?? [];
    }
    if (!edgesRows.length) {
      const res3 = await buildStaleQuery();
      if (res3.error && !error) error = res3.error;
      edgesRows = res3.data ?? [];
    }
    edgesRows = edgesRows.map((row: any) => {
      const odds = coerceDecimalOdds(row);
      return Number.isFinite(odds) ? { ...row, odds } : row;
    });

    let manualRows: any[] = [];
    if (includeManual && (!source || source === "all" || source === "manual")) {
      let manualQuery = sb
        .from("manual_odds")
        .select("*")
        .order("pulled_at", { ascending: false })
        .limit(500);
      if (market !== "all") {
        manualQuery = manualQuery.eq("market", market);
      }

      if (q) manualQuery = manualQuery.ilike("player_name", `%${q}%`);

      const manualRes = await manualQuery;
      const manualRaw = (manualRes.data || []) as ManualOddsRow[];
      const minCreatedTs = minCreated.getTime();

      manualRows = buildManualEdges(
        manualRaw.filter((row) => {
          if (book && String(row.book || "").toLowerCase() !== book) return false;
          const ts = new Date(row.pulled_at || row.created_at || 0).getTime();
          if (!Number.isFinite(ts) || ts === 0) return false;
          if (ts < minCreatedTs) return false;
          return true;
        })
      );
    }

    let combined = edgesRows.concat(manualRows);

    // Player points: keep the consensus line per player/event (drops truncated/alt anomalies)
    if (combined.length) {
      const consensus = new Map<string, number>();
      const lineCounts = new Map<string, Map<number, number>>();
      for (const row of combined) {
        if (row.market !== "player_points") continue;
        const key = [row.event_id || "", row.player_name || "", row.market || ""].join("|");
        const line = Number(row.line);
        if (!Number.isFinite(line)) continue;
        if (!lineCounts.has(key)) lineCounts.set(key, new Map());
        const m = lineCounts.get(key)!;
        m.set(line, (m.get(line) || 0) + 1);
      }
      lineCounts.forEach((counts, key) => {
        let bestLine = 0;
        let bestCount = -1;
        const lines = Array.from(counts.keys()).sort((a, b) => a - b);
        if (!lines.length) return;
        const mid = Math.floor(lines.length / 2);
        const median = lines.length % 2 === 0 ? (lines[mid - 1] + lines[mid]) / 2 : lines[mid];
        counts.forEach((count, line) => {
          if (count > bestCount) {
            bestCount = count;
            bestLine = line;
            return;
          }
          if (count === bestCount) {
            const bestDist = Math.abs(bestLine - median);
            const lineDist = Math.abs(line - median);
            if (lineDist < bestDist || (lineDist === bestDist && line < bestLine)) {
              bestLine = line;
            }
          }
        });
        if (bestCount > 0) consensus.set(key, bestLine);
      });
      combined = combined.filter((row) => {
        if (row.market !== "player_points") return true;
        const key = [row.event_id || "", row.player_name || "", row.market || ""].join("|");
        const line = Number(row.line);
        const bestLine = consensus.get(key);
        if (!Number.isFinite(line) || bestLine === undefined) return true;
        return line === bestLine;
      });
    }

    if (onlyPos || Number.isFinite(evMin)) {
      combined = combined.filter((row) => Number(row.ev) >= evMin);
    }

    if (source && source !== "all") {
      if (source === "model") {
        combined = combined.filter((row) => String(row.source || "").toLowerCase().startsWith("points_model"));
      } else {
        combined = combined.filter((row) => String(row.source || "").toLowerCase() === source);
      }
    }

    combined.sort((a, b) => {
      const av = sortColumn === "created_at" ? new Date(a.created_at || 0).getTime() : Number(a[sortColumn] || 0);
      const bv = sortColumn === "created_at" ? new Date(b.created_at || 0).getTime() : Number(b[sortColumn] || 0);
      return dir ? av - bv : bv - av;
    });

    const total = combined.length;
    const pageRows = combined.slice(rangeStart, rangeEnd + 1);

    return NextResponse.json(
      { data: pageRows ?? [], total, error: error?.message ?? null },
      { headers: { "Cache-Control": "no-store" } }
    );
  } catch (e: any) {
    return NextResponse.json(
      { data: [], total: 0, error: e?.message ?? "Unknown error" },
      { status: 500 }
    );
  }
}
