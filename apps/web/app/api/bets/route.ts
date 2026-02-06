import { NextResponse } from "next/server";
import { supabaseServer } from "@/lib/supabase/server";

function parseNumber(value: any) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function computeProfit(result: string | null, stake: number, odds: number) {
  if (!result) return null;
  const r = result.toLowerCase();
  if (r === "win") return stake * (odds - 1);
  if (r === "lose") return -stake;
  if (r === "push") return 0;
  return null;
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const limit = Math.min(Number(searchParams.get("limit") || "200"), 1000);
    const sb = supabaseServer();

    const { data, error } = await sb
      .from("bets")
      .select("*")
      .order("created_at", { ascending: false })
      .limit(limit);

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
    const side = String(body.side || "").trim().toLowerCase();
    const line = parseNumber(body.line);
    const odds = parseNumber(body.odds);
    const stake = parseNumber(body.stake);
    const unit = parseNumber(body.unit);
    const book = String(body.book || "").trim();
    const source = String(body.source || "").trim();
    const starts_at = body.starts_at || null;
    const result = body.result ? String(body.result).trim().toLowerCase() : null;
    const notes = body.notes ? String(body.notes).trim() : null;

    if (!player_name || !market || !side || line === null || odds === null || stake === null) {
      return NextResponse.json({ error: "Missing required fields" }, { status: 400 });
    }

    const profit = computeProfit(result, stake, odds);

    const sb = supabaseServer();
    const { data, error } = await sb
      .from("bets")
      .insert([
        {
          player_name,
          market,
          side,
          line,
          odds,
          stake,
          unit,
          book,
          source,
          starts_at,
          result,
          profit,
          notes,
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

export async function PATCH(req: Request) {
  try {
    const body = await req.json();
    const id = String(body.id || "").trim();
    const result = body.result ? String(body.result).trim().toLowerCase() : null;
    const stake = parseNumber(body.stake);
    const odds = parseNumber(body.odds);

    if (!id || !result || stake === null || odds === null) {
      return NextResponse.json({ error: "Missing required fields" }, { status: 400 });
    }

    const profit = computeProfit(result, stake, odds);
    const sb = supabaseServer();
    const { data, error } = await sb
      .from("bets")
      .update({ result, profit })
      .eq("id", id)
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
