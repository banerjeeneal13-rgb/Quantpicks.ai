import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

function parseNumber(value: any) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const limit = Math.min(Number(searchParams.get("limit") || "200"), 1000);
    const market = (searchParams.get("market") || "").trim();
    const q = (searchParams.get("q") || "").trim();
    const book = (searchParams.get("book") || "").trim().toLowerCase();

    let query = supabaseServer()
      .from("manual_odds")
      .select("*")
      .order("pulled_at", { ascending: false })
      .limit(limit);

    if (market) query = query.eq("market", market);
    if (book) query = query.eq("book", book);
    if (q) query = query.ilike("player_name", `%${q}%`);

    const { data, error } = await query;
    return NextResponse.json({ data: data ?? [], error: error?.message ?? null });
  } catch (e: any) {
    return NextResponse.json({ data: [], error: e?.message ?? "Unknown error" }, { status: 500 });
  }
}

export async function POST(req: Request) {
  try {
    const body = await req.json();
    const player_name = String(body.player_name || "").trim();
    const market = String(body.market || "").trim();
    const line = parseNumber(body.line);
    const over_odds = parseNumber(body.over_odds);
    const under_odds = parseNumber(body.under_odds);
    const book = String(body.book || "").trim().toLowerCase();
    const pulled_at = body.pulled_at ? String(body.pulled_at).trim() : new Date().toISOString();
    const game_date = body.game_date ? String(body.game_date).trim() : null;
    const game = body.game ? String(body.game).trim() : null;
    const event_id = body.event_id ? String(body.event_id).trim() : null;
    const notes = body.notes ? String(body.notes).trim() : null;

    if (!player_name || !market || line === null || !book) {
      return NextResponse.json({ error: "Missing required fields" }, { status: 400 });
    }
    if (over_odds === null && under_odds === null) {
      return NextResponse.json({ error: "Provide at least one of over_odds or under_odds" }, { status: 400 });
    }

    const { data, error } = await supabaseServer()
      .from("manual_odds")
      .insert([
        {
          player_name,
          market,
          line,
          over_odds,
          under_odds,
          book,
          pulled_at,
          game_date,
          game,
          event_id,
          notes,
          source: "manual",
        },
      ])
      .select("*")
      .single();

    if (error) {
      return NextResponse.json({ error: error.message }, { status: 400 });
    }

    return NextResponse.json({ data, error: null });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? "Unknown error" }, { status: 500 });
  }
}
