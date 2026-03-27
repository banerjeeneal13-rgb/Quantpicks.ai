import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import { coerceDecimalOdds } from "@/lib/odds";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

/**
 * Premium pick thresholds — a pick qualifies as "on record" if it meets EITHER:
 *   EV >= 0.05  OR  P >= 0.57
 * This creates a variable number of daily picks (could be 1 or 15+).
 */
const PREMIUM_EV_THRESHOLD = 0.05;
const PREMIUM_P_THRESHOLD = 0.57;

function supabaseAdmin() {
  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
}

function todayStr() {
  return new Date().toISOString().slice(0, 10);
}

// ── Server-side in-memory cache (fallback if daily_picks table doesn't exist)
let _memoryCache: {
  date: string;
  top3: any[];
  premium: any[];
  all: any[];
} | null = null;

/**
 * GET /api/daily-picks?type=top3|premium|all
 *
 * Returns the daily snapshot of picks. Once set for a day, picks do not change
 * until the next calendar day. On the first request of a new day, a fresh
 * snapshot is created from the current edges.
 */
export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const pickType = searchParams.get("type") || "all";
    const today = todayStr();

    const sb = supabaseAdmin();

    // 1) Check in-memory cache first (works even without daily_picks table)
    if (_memoryCache && _memoryCache.date === today) {
      return respond(pickType, today, _memoryCache.top3, _memoryCache.premium, _memoryCache.all);
    }

    // 2) Try to read from daily_picks table (may not exist yet)
    let existing: any = null;
    try {
      const { data } = await sb
        .from("daily_picks")
        .select("*")
        .eq("pick_date", today)
        .limit(1)
        .maybeSingle();
      existing = data;
    } catch {
      // Table doesn't exist yet — that's OK, we'll fall through
    }

    if (existing) {
      const top3 = existing.top3_picks || [];
      const premium = existing.premium_picks || [];
      const allPicks = existing.all_picks || [];
      _memoryCache = { date: today, top3, premium, all: allPicks };
      return respond(pickType, today, top3, premium, allPicks);
    }

    // 3) No snapshot yet — create one from current edges
    //    Include all games for today (even those that started) so picks persist
    const todayStart = `${today}T00:00:00.000Z`;
    const tomorrowEnd = new Date(new Date(today + "T00:00:00Z").getTime() + 36 * 60 * 60 * 1000).toISOString();

    const { data: edges } = await sb
      .from("edges")
      .select("*")
      .gte("starts_at", todayStart)
      .lte("starts_at", tomorrowEnd)
      .order("ev", { ascending: false })
      .limit(300);

    let allEdges = (edges || []).map((row: any) => {
      const odds = coerceDecimalOdds(row);
      const p = Number(row.p);
      const ev = Number(row.ev);
      return {
        id: row.id,
        player_name: row.player_name,
        market: row.market,
        side: row.side,
        line: Number(row.line),
        odds: Number.isFinite(odds) ? odds : Number(row.odds),
        p,
        ev,
        book: row.book,
        source: row.source,
        starts_at: row.starts_at,
        team_abbr: row.team_abbr || null,
        event_id: row.event_id || null,
      };
    });

    // Sort by EV descending
    allEdges.sort((a: any, b: any) => (b.ev || 0) - (a.ev || 0));

    // Top 3 picks: highest EV
    const top3 = allEdges.slice(0, 3);

    // Premium picks: meet either EV or P threshold
    const premium = allEdges.filter(
      (e: any) => e.ev >= PREMIUM_EV_THRESHOLD || e.p >= PREMIUM_P_THRESHOLD
    );

    const allSlice = allEdges.slice(0, 50);

    // Cache in memory immediately
    _memoryCache = { date: today, top3, premium, all: allSlice };

    // 4) Try to persist to daily_picks table (best-effort)
    try {
      await sb.from("daily_picks").upsert(
        {
          pick_date: today,
          top3_picks: top3,
          premium_picks: premium,
          all_picks: allSlice,
          top3_count: top3.length,
          premium_count: premium.length,
          created_at: new Date().toISOString(),
        },
        { onConflict: "pick_date" }
      );
    } catch {
      // Table may not exist yet — that's OK, memory cache still works
    }

    return respond(pickType, today, top3, premium, allSlice);
  } catch (e: any) {
    return NextResponse.json(
      { error: e?.message ?? "Unknown error", top3: [], premium: [], all: [] },
      { status: 500 }
    );
  }
}

function respond(pickType: string, date: string, top3: any[], premium: any[], all: any[]) {
  if (pickType === "top3") {
    return NextResponse.json({ data: top3, date, count: top3.length, type: "top3" });
  }
  if (pickType === "premium") {
    return NextResponse.json({ data: premium, date, count: premium.length, type: "premium" });
  }
  return NextResponse.json({
    top3,
    premium,
    all,
    date,
    top3Count: top3.length,
    premiumCount: premium.length,
  });
}
