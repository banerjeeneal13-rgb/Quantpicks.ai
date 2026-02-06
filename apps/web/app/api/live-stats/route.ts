import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const market = searchParams.get("market") || "player_points";
    const limit = Math.min(Number(searchParams.get("limit") || "200"), 500);
    const lookbackHours = Math.max(1, Math.min(Number(searchParams.get("lookbackHours") || "6"), 24));

    const now = new Date();
    const minStart = new Date(now.getTime() - lookbackHours * 60 * 60 * 1000);
    const maxStart = new Date(now.getTime() + 12 * 60 * 60 * 1000);

    const sb = supabaseServer();
    const { data: edges, error: edgesError } = await sb
      .from("edges")
      .select("*")
      .eq("market", market)
      .gte("starts_at", minStart.toISOString())
      .lte("starts_at", maxStart.toISOString())
      .order("starts_at", { ascending: true })
      .limit(limit);

    if (edgesError) {
      return NextResponse.json({ data: [], error: edgesError.message }, { status: 500 });
    }

    const { data: live, error: liveError } = await sb
      .from("live_player_stats")
      .select("*")
      .order("updated_at", { ascending: false })
      .limit(2000);

    if (liveError) {
      return NextResponse.json({ data: [], error: liveError.message }, { status: 500 });
    }

    const liveMap = new Map(
      (live || []).map((r: any) => [String(r.player_name || "").toLowerCase(), r])
    );

    const rows = (edges || []).map((e: any) => {
      const liveRow = liveMap.get(String(e.player_name || "").toLowerCase());
      let current = null;
      if (liveRow) {
        if (market === "player_points") current = Number(liveRow.pts);
        if (market === "player_rebounds") current = Number(liveRow.reb);
        if (market === "player_assists") current = Number(liveRow.ast);
      }
      const line = Number(e.line);
      const progress = current !== null && Number.isFinite(line) && line > 0 ? current / line : null;
      return {
        ...e,
        current,
        progress,
        updated_at: liveRow?.updated_at || null,
        minutes: liveRow?.minutes || null,
        team_abbr: liveRow?.team_abbr || null,
      };
    });

    return NextResponse.json({ data: rows, error: null });
  } catch (e: any) {
    return NextResponse.json({ data: [], error: e?.message ?? "Unknown error" }, { status: 500 });
  }
}
