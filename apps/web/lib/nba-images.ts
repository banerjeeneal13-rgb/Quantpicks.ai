/**
 * NBA player headshot and team logo utilities.
 *
 * Player headshots use the NBA CDN: cdn.nba.com/headshots/nba/latest/260x190/{PERSON_ID}.png
 * Team logos use the NBA CDN: cdn.nba.com/logos/nba/{TEAM_ID}/primary/L/logo.svg
 *
 * For player name -> person_id resolution we maintain a server-side cache
 * populated via /api/player-headshot.  Client components should use the
 * <PlayerAvatar> and <TeamLogo> helpers exported here.
 */

// ── Team abbreviation → NBA team ID mapping ─────────────────────────────────
export const TEAM_ID_MAP: Record<string, number> = {
  ATL: 1610612737,
  BOS: 1610612738,
  BKN: 1610612751,
  CHA: 1610612766,
  CHI: 1610612741,
  CLE: 1610612739,
  DAL: 1610612742,
  DEN: 1610612743,
  DET: 1610612765,
  GSW: 1610612744,
  HOU: 1610612745,
  IND: 1610612754,
  LAC: 1610612746,
  LAL: 1610612747,
  MEM: 1610612763,
  MIA: 1610612748,
  MIL: 1610612749,
  MIN: 1610612750,
  NOP: 1610612740,
  NYK: 1610612752,
  OKC: 1610612760,
  ORL: 1610612753,
  PHI: 1610612755,
  PHX: 1610612756,
  POR: 1610612757,
  SAC: 1610612758,
  SAS: 1610612759,
  TOR: 1610612761,
  UTA: 1610612762,
  WAS: 1610612764,
};

/** Get a team logo URL from team abbreviation. */
export function teamLogoUrl(teamAbbr: string): string {
  const abbr = (teamAbbr || "").toUpperCase().trim();
  const id = TEAM_ID_MAP[abbr];
  if (id) {
    return `https://cdn.nba.com/logos/nba/${id}/primary/L/logo.svg`;
  }
  // fallback to ESPN pattern which also works well
  return `https://a.espn.com/i/teamlogos/nba/500/${abbr.toLowerCase()}.png`;
}

/** Generate initials from a player or team name. */
export function nameInitials(name: string): string {
  if (!name) return "?";
  const parts = name.trim().split(/\s+/);
  if (parts.length >= 2) {
    return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
  }
  return name.slice(0, 2).toUpperCase();
}

/**
 * Build an NBA CDN headshot URL from a person ID.
 * Returns null if no ID provided.
 */
export function nbaHeadshotUrl(personId: number | string | null | undefined): string | null {
  if (!personId) return null;
  return `https://cdn.nba.com/headshots/nba/latest/260x190/${personId}.png`;
}

// ── Client-side headshot cache ──────────────────────────────────────────────
const _headshotCache = new Map<string, string | null>();

/**
 * Fetch a player's headshot URL via our /api/player-headshot endpoint.
 * Caches results in memory so we only look up each player once per session.
 */
export async function fetchPlayerHeadshot(playerName: string): Promise<string | null> {
  const key = playerName.toLowerCase().trim();
  if (_headshotCache.has(key)) return _headshotCache.get(key) ?? null;

  try {
    const res = await fetch(`/api/player-headshot?name=${encodeURIComponent(key)}`);
    if (!res.ok) {
      _headshotCache.set(key, null);
      return null;
    }
    const json = await res.json();
    const url = json.url || null;
    _headshotCache.set(key, url);
    return url;
  } catch {
    _headshotCache.set(key, null);
    return null;
  }
}

/**
 * Check if a market is a team-level prop (moneyline, spread, total)
 * vs a player-level prop.
 */
export function isTeamMarket(market: string): boolean {
  const m = (market || "").toLowerCase();
  return m === "moneyline" || m === "spread" || m === "total";
}
