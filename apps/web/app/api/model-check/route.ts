import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

function supabaseAdmin() {
  if (!SUPABASE_URL) throw new Error("Missing SUPABASE_URL");
  if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
}

export async function GET() {
  try {
    const sb = supabaseAdmin();

    const latestModelEdge = sb
      .from("edges")
      .select("player_name,market,side,line,odds,p,ev,source,created_at,starts_at")
      .eq("market", "player_points")
      .ilike("source", "points_model%")
      .order("created_at", { ascending: false })
      .limit(1);

    const latestAnyEdge = sb
      .from("edges")
      .select("player_name,market,side,line,odds,p,ev,source,created_at,starts_at")
      .order("created_at", { ascending: false })
      .limit(1);

    const modelCounts = sb
      .from("edges")
      .select("id", { count: "exact", head: true })
      .eq("market", "player_points")
      .ilike("source", "points_model%");

    const predCounts = sb
      .from("predictions")
      .select("id", { count: "exact", head: true });

    const settledCounts = sb
      .from("predictions")
      .select("id", { count: "exact", head: true })
      .not("actual_value", "is", null);

    const [latestModelRes, latestAnyRes, modelCountsRes, predCountsRes, settledCountsRes] =
      await Promise.all([latestModelEdge, latestAnyEdge, modelCounts, predCounts, settledCounts]);

    return NextResponse.json({
      latest_model_edge: (latestModelRes.data || [])[0] || null,
      latest_any_edge: (latestAnyRes.data || [])[0] || null,
      counts: {
        player_points_model_edges: modelCountsRes.count ?? 0,
        predictions_total: predCountsRes.count ?? 0,
        predictions_with_actuals: settledCountsRes.count ?? 0,
      },
      errors: {
        latest_model_edge: latestModelRes.error?.message ?? null,
        latest_any_edge: latestAnyRes.error?.message ?? null,
        model_counts: modelCountsRes.error?.message ?? null,
        predictions_total: predCountsRes.error?.message ?? null,
        predictions_with_actuals: settledCountsRes.error?.message ?? null,
      },
    });
  } catch (e: any) {
    return NextResponse.json({ error: e?.message ?? "Unknown error" }, { status: 500 });
  }
}
