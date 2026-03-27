import { NextResponse } from "next/server";

export const dynamic = "force-dynamic";

/**
 * GET /api/player-headshot?name=LeBron+James
 *
 * Resolves a player name to a headshot URL using the ESPN athlete search API.
 * Results are cached in-memory on the server for the lifetime of the process.
 */

const cache = new Map<string, string | null>();

const ESPN_SEARCH =
  "https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes";

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const name = (searchParams.get("name") || "").trim().toLowerCase();

    if (!name) {
      return NextResponse.json({ url: null, error: "Missing name" }, { status: 400 });
    }

    // Check cache first
    if (cache.has(name)) {
      return NextResponse.json({ url: cache.get(name), cached: true });
    }

    // Search ESPN for the player
    const url = `${ESPN_SEARCH}?limit=1&search=${encodeURIComponent(name)}`;
    const res = await fetch(url, {
      headers: { "User-Agent": "QuantpicksAI/1.0" },
      signal: AbortSignal.timeout(5000),
    });

    if (!res.ok) {
      cache.set(name, null);
      return NextResponse.json({ url: null, error: "ESPN search failed" });
    }

    const json = await res.json();
    const athletes = json?.items || json?.athletes || [];
    const athlete = athletes[0];

    if (!athlete) {
      cache.set(name, null);
      return NextResponse.json({ url: null, error: "Player not found" });
    }

    // ESPN provides headshot in the athlete object
    let headshot: string | null = null;

    // Try direct headshot field
    if (athlete.headshot?.href) {
      headshot = athlete.headshot.href;
    } else if (athlete.id) {
      // Construct ESPN headshot URL from athlete ID
      headshot = `https://a.espn.com/combiner/i?img=/i/headshots/nba/players/full/${athlete.id}.png&w=350&h=254`;
    }

    cache.set(name, headshot);
    return NextResponse.json({ url: headshot, id: athlete.id || null });
  } catch (e: any) {
    return NextResponse.json(
      { url: null, error: e?.message ?? "Unknown error" },
      { status: 500 }
    );
  }
}
