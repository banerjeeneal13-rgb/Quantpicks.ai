import { NextResponse } from "next/server";
import { createClient } from "@supabase/supabase-js";
import fs from "fs";
import path from "path";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_ROLE_KEY = process.env.SUPABASE_SERVICE_ROLE_KEY!;

function supabaseAdmin() {
  if (!SUPABASE_URL) throw new Error("Missing SUPABASE_URL");
  if (!SUPABASE_SERVICE_ROLE_KEY) throw new Error("Missing SUPABASE_SERVICE_ROLE_KEY");

  return createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {
    auth: { persistSession: false },
  });
}

type CacheRow = Record<string, string>;

const REPO_ROOT = path.resolve(process.cwd(), "..", "..");
const MODEL_ENV_PATH = path.join(REPO_ROOT, "services", "model", ".env");
const LOGS_PATH = path.join(REPO_ROOT, "services", "model", "data", "nba_player_logs_points_all.csv");
const ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba";
const ESPN_CORE = "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba";

const TEAM_NAME_TO_ABBR: Record<string, string> = {
  "Atlanta Hawks": "ATL",
  "Boston Celtics": "BOS",
  "Brooklyn Nets": "BKN",
  "Charlotte Hornets": "CHA",
  "Chicago Bulls": "CHI",
  "Cleveland Cavaliers": "CLE",
  "Dallas Mavericks": "DAL",
  "Denver Nuggets": "DEN",
  "Detroit Pistons": "DET",
  "Golden State Warriors": "GSW",
  "Houston Rockets": "HOU",
  "Indiana Pacers": "IND",
  "LA Clippers": "LAC",
  "Los Angeles Clippers": "LAC",
  "Los Angeles Lakers": "LAL",
  "Memphis Grizzlies": "MEM",
  "Miami Heat": "MIA",
  "Milwaukee Bucks": "MIL",
  "Minnesota Timberwolves": "MIN",
  "New Orleans Pelicans": "NOP",
  "New York Knicks": "NYK",
  "Oklahoma City Thunder": "OKC",
  "Orlando Magic": "ORL",
  "Philadelphia 76ers": "PHI",
  "Phoenix Suns": "PHX",
  "Portland Trail Blazers": "POR",
  "Sacramento Kings": "SAC",
  "San Antonio Spurs": "SAS",
  "Toronto Raptors": "TOR",
  "Utah Jazz": "UTA",
  "Washington Wizards": "WAS",
};

const TEAM_ABBR_TO_NAME = Object.fromEntries(
  Object.entries(TEAM_NAME_TO_ABBR).map(([name, abbr]) => [abbr, name])
);

function parseCsvRow(line: string) {
  const out: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === "\"") {
      inQuotes = !inQuotes;
      continue;
    }
    if (ch === "," && !inQuotes) {
      out.push(cur);
      cur = "";
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

function parseGameDate(raw: string) {
  if (!raw) return null;
  const s = raw.trim().replace(/^"|"$/g, "");
  if (!s) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(s)) {
    const d = new Date(`${s}T00:00:00Z`);
    if (!Number.isNaN(d.getTime())) return d;
  }
  const d2 = new Date(s);
  if (!Number.isNaN(d2.getTime())) return d2;
  return null;
}

function computeLast10FromLogs(playerName: string) {
  if (!fs.existsSync(LOGS_PATH)) return null;
  const raw = fs.readFileSync(LOGS_PATH, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const nameIdx = header.indexOf("player_name");
  const dateIdx = header.indexOf("GAME_DATE");
  const minIdx = header.indexOf("MIN");
  const ptsIdx = header.indexOf("PTS");
  const fgaIdx = header.indexOf("FGA");
  if ([nameIdx, dateIdx, minIdx, ptsIdx, fgaIdx].some((i) => i < 0)) return null;

  const rows: { date: Date; min: number; pts: number; fga: number }[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    if (parts[nameIdx] !== playerName) continue;
    const d = parseGameDate(parts[dateIdx] || "");
    if (!d) continue;
    const min = Number(parts[minIdx]);
    const pts = Number(parts[ptsIdx]);
    const fga = Number(parts[fgaIdx]);
    if (!Number.isFinite(min) || !Number.isFinite(pts) || !Number.isFinite(fga)) continue;
    if (min < 5) continue;
    rows.push({ date: d, min, pts, fga });
  }
  if (!rows.length) return null;
  rows.sort((a, b) => a.date.getTime() - b.date.getTime());
  const last10 = rows.slice(-10);
  const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const mins = last10.map((r) => r.min);
  const pts = last10.map((r) => r.pts);
  const fga = last10.map((r) => r.fga);
  const asOf = last10[last10.length - 1].date.toISOString().slice(0, 10);
  return {
    pts: avg(pts),
    min: avg(mins),
    fga: avg(fga),
    asOf,
  };
}

async function fetchJson(url: string) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`ESPN fetch failed: ${res.status}`);
  return res.json();
}

function seasonYearFromDate(d: Date) {
  const year = d.getFullYear();
  return d.getMonth() + 1 >= 8 ? year + 1 : year;
}

async function fetchEspnLast10(playerName: string, teamAbbr?: string | null, startsAt?: string | null) {
  if (!playerName || !teamAbbr) return null;
  const teams = await fetchJson(`${ESPN_BASE}/teams`);
  const teamList = teams?.sports?.[0]?.leagues?.[0]?.teams || [];
  let teamId: string | null = null;
  for (const item of teamList) {
    const team = item?.team || {};
    if (String(team.abbreviation || "").toUpperCase() === String(teamAbbr).toUpperCase()) {
      teamId = String(team.id || "");
      break;
    }
  }
  if (!teamId) return null;

  const roster = await fetchJson(`${ESPN_BASE}/teams/${teamId}/roster`);
  const athletes = roster?.athletes || [];
  let athleteId: string | null = null;
  const target = String(playerName).toLowerCase();
  for (const a of athletes) {
    const name = String(a.displayName || "").toLowerCase();
    if (name === target) {
      athleteId = String(a.id || "");
      break;
    }
  }
  if (!athleteId) return null;

  const refDate = startsAt ? new Date(startsAt) : new Date();
  const seasonYear = seasonYearFromDate(refDate);
  const eventlogBase = `${ESPN_CORE}/seasons/${seasonYear}/athletes/${athleteId}/eventlog?lang=en&region=us`;

  const first = await fetchJson(eventlogBase);
  const eventsMeta = first?.events || {};
  const pageCount = Number(eventsMeta.pageCount || 1);
  const allItems: any[] = [];
  for (let page = 1; page <= pageCount; page += 1) {
    const url = page === 1 ? eventlogBase : `${eventlogBase}&page=${page}`;
    const data = page === 1 ? first : await fetchJson(url);
    const items = data?.events?.items || [];
    allItems.push(...items);
  }

  if (!allItems.length) return null;

  const rows: { date: Date; min: number; pts: number; fga: number }[] = [];
  for (const item of allItems) {
    if (!item?.played) continue;
    const eventRef = item?.event?.$ref;
    const statRef = item?.statistics?.$ref;
    if (!eventRef || !statRef) continue;
    const event = await fetchJson(eventRef);
    const dateStr = event?.date;
    if (!dateStr) continue;
    const date = new Date(String(dateStr).replace("Z", "+00:00"));
    const stat = await fetchJson(statRef);
    const cats = stat?.splits?.categories || [];
    const flat: Record<string, any> = {};
    for (const cat of cats) {
      for (const s of cat?.stats || []) {
        if (s?.name) flat[s.name] = s;
      }
    }
    const min = Number(flat.minutes?.value ?? 0);
    const pts = Number(flat.points?.value ?? 0);
    const fga = Number(flat.fieldGoalsAttempted?.value ?? 0);
    if (!Number.isFinite(min) || min < 5) continue;
    rows.push({ date, min, pts, fga });
  }

  if (!rows.length) return null;
  rows.sort((a, b) => a.date.getTime() - b.date.getTime());
  const last10 = rows.slice(-10);
  const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const mins = last10.map((r) => r.min);
  const pts = last10.map((r) => r.pts);
  const fga = last10.map((r) => r.fga);
  const asOf = last10[last10.length - 1].date.toISOString().slice(0, 10);
  return {
    pts: avg(pts),
    min: avg(mins),
    fga: avg(fga),
    asOf,
  };
}
function parseFeatureCacheRow(playerName: string): CacheRow | null {
  const cachePath = path.join(REPO_ROOT, "services", "model", "data", "points_feature_cache.csv");
  if (!fs.existsSync(cachePath)) return null;
  const raw = fs.readFileSync(cachePath, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const nameIdx = header.indexOf("player_name");
  if (nameIdx === -1) return null;
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    if (parts[nameIdx] === playerName) {
      const row: Record<string, string> = {};
      header.forEach((h, idx) => {
        row[h] = parts[idx] ?? "";
      });
      return row;
    }
  }
  return null;
}

function impliedProbFromDecimal(decimalOdds: number) {
  if (!decimalOdds || decimalOdds <= 1) return null;
  return 1 / decimalOdds;
}

function formatPercent(value: number | null) {
  if (value === null || !Number.isFinite(value)) return null;
  const v = value <= 1 ? value * 100 : value;
  return v;
}

function opponentAbbrFromMatchup(matchup?: string | null, teamAbbr?: string | null) {
  if (!matchup) return null;
  const parts = matchup.split(" ");
  if (parts.length < 3) return null;
  const home = parts[0];
  const away = parts[2];
  if (!teamAbbr) return null;
  if (teamAbbr === home) return away;
  if (teamAbbr === away) return home;
  return null;
}

function teamAbbrFromMatchup(matchup?: string | null) {
  if (!matchup) return null;
  const parts = matchup.split(" ");
  if (!parts.length) return null;
  return parts[0] || null;
}

function computeH2HFromLogs(playerName: string, oppAbbr: string) {
  if (!fs.existsSync(LOGS_PATH)) return null;
  const raw = fs.readFileSync(LOGS_PATH, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const nameIdx = header.indexOf("player_name");
  const dateIdx = header.indexOf("GAME_DATE");
  const minIdx = header.indexOf("MIN");
  const ptsIdx = header.indexOf("PTS");
  const fgaIdx = header.indexOf("FGA");
  const matchupIdx = header.indexOf("MATCHUP");
  if ([nameIdx, dateIdx, minIdx, ptsIdx, fgaIdx, matchupIdx].some((i) => i < 0)) return null;

  const rows: { date: Date; min: number; pts: number; fga: number }[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    if (parts[nameIdx] !== playerName) continue;
    const matchup = parts[matchupIdx] || "";
    if (!matchup.includes(` ${oppAbbr}`)) continue;
    const d = parseGameDate(parts[dateIdx] || "");
    if (!d) continue;
    const min = Number(parts[minIdx]);
    const pts = Number(parts[ptsIdx]);
    const fga = Number(parts[fgaIdx]);
    if (!Number.isFinite(min) || !Number.isFinite(pts) || !Number.isFinite(fga)) continue;
    if (min < 5) continue;
    rows.push({ date: d, min, pts, fga });
  }
  if (!rows.length) return null;
  rows.sort((a, b) => a.date.getTime() - b.date.getTime());
  const last5 = rows.slice(-5);
  const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const mins = last5.map((r) => r.min);
  const pts = last5.map((r) => r.pts);
  const fga = last5.map((r) => r.fga);
  return {
    count: last5.length,
    pts: avg(pts),
    min: avg(mins),
    fga: avg(fga),
    asOf: last5[last5.length - 1].date.toISOString().slice(0, 10),
  };
}

function computeWithoutTeammatesFromLogs(
  playerName: string,
  teamAbbr: string,
  teammateNames: string[]
) {
  if (!fs.existsSync(LOGS_PATH)) return null;
  const teammateSet = new Set(teammateNames.map((n) => n.trim()).filter(Boolean));
  if (!teammateSet.size) return null;

  const raw = fs.readFileSync(LOGS_PATH, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const nameIdx = header.indexOf("player_name");
  const dateIdx = header.indexOf("GAME_DATE");
  const minIdx = header.indexOf("MIN");
  const ptsIdx = header.indexOf("PTS");
  const fgaIdx = header.indexOf("FGA");
  const matchupIdx = header.indexOf("MATCHUP");
  if ([nameIdx, dateIdx, minIdx, ptsIdx, fgaIdx, matchupIdx].some((i) => i < 0)) return null;

  const playerRows: { date: Date; min: number; pts: number; fga: number }[] = [];
  const teammateDates: Record<string, Set<string>> = {};
  teammateSet.forEach((n) => {
    teammateDates[n] = new Set<string>();
  });

  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    const name = parts[nameIdx];
    const matchup = parts[matchupIdx] || "";
    const team = teamAbbrFromMatchup(matchup);
    const d = parseGameDate(parts[dateIdx] || "");
    if (!d) continue;
    const dateStr = d.toISOString().slice(0, 10);

    if (name === playerName) {
      const min = Number(parts[minIdx]);
      const pts = Number(parts[ptsIdx]);
      const fga = Number(parts[fgaIdx]);
      if (!Number.isFinite(min) || !Number.isFinite(pts) || !Number.isFinite(fga)) continue;
      if (min < 5) continue;
      playerRows.push({ date: d, min, pts, fga });
    } else if (teammateSet.has(name) && team && team.toUpperCase() === teamAbbr.toUpperCase()) {
      teammateDates[name].add(dateStr);
    }
  }

  if (!playerRows.length) return null;
  playerRows.sort((a, b) => a.date.getTime() - b.date.getTime());
  const window = 25;
  const lastRows = playerRows.slice(-window);
  const asOf = lastRows[lastRows.length - 1].date.toISOString().slice(0, 10);

  const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const out: { name: string; count: number; pts: number; min: number; fga: number; asOf: string }[] = [];

  teammateSet.forEach((name) => {
    const dates = teammateDates[name] || new Set<string>();
    const without = lastRows.filter((r) => !dates.has(r.date.toISOString().slice(0, 10)));
    if (!without.length) return;
    out.push({
      name,
      count: without.length,
      pts: avg(without.map((r) => r.pts)),
      min: avg(without.map((r) => r.min)),
      fga: avg(without.map((r) => r.fga)),
      asOf,
    });
  });

  return out.length ? out : null;
}

function getOpponentName(event: any, teamAbbr?: string) {
  const home = event?.teams?.home?.names?.long;
  const away = event?.teams?.away?.names?.long;
  if (!teamAbbr || !home || !away) return null;
  const homeAbbr = TEAM_NAME_TO_ABBR[home] || null;
  const awayAbbr = TEAM_NAME_TO_ABBR[away] || null;
  if (!homeAbbr || !awayAbbr) return null;
  if (teamAbbr === homeAbbr) return away;
  if (teamAbbr === awayAbbr) return home;
  return null;
}

function getMatchupLabel(event: any) {
  const home = event?.teams?.home?.names?.long;
  const away = event?.teams?.away?.names?.long;
  if (home && away) return `${away} at ${home}`;
  return null;
}

function loadDefenderRatings() {
  const p = path.join(REPO_ROOT, "services", "model", "data", "defender_ratings.csv");
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const out: CacheRow[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    const row: CacheRow = {};
    header.forEach((h, idx) => {
      row[h] = parts[idx] ?? "";
    });
    out.push(row);
  }
  return out;
}

function loadOffenseMatchups() {
  const p = path.join(REPO_ROOT, "services", "model", "matchups_offense_active_2025-26_Regular_Season.csv");
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const out: CacheRow[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    const row: CacheRow = {};
    header.forEach((h, idx) => {
      row[h] = parts[idx] ?? "";
    });
    out.push(row);
  }
  return out;
}

function loadDefenseMatchups() {
  const p = path.join(REPO_ROOT, "services", "model", "matchups_defense_active_2025-26_Regular_Season.csv");
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const out: CacheRow[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    const row: CacheRow = {};
    header.forEach((h, idx) => {
      row[h] = parts[idx] ?? "";
    });
    out.push(row);
  }
  return out;
}

function loadDefenseMatchupStats(playerName: string, oppAbbr: string) {
  const defRows = loadDefenseMatchups();
  if (!defRows) return null;
  const rows = defRows
    .filter((r) => r.FOCUS_PLAYER_NAME === playerName && String(r.DEF_TEAM_ABBREVIATION || "").toUpperCase() === oppAbbr.toUpperCase())
    .map((r) => ({
      name: String(r.DEF_PLAYER_NAME || "").trim(),
      pct: Number(r.PERCENT_OF_TIME),
      fgPct: Number(r.MATCHUP_FG_PCT) || null,
      fg3Pct: Number(r.MATCHUP_FG3_PCT) || null,
      fga: Number(r.MATCHUP_FGA) || null,
      min: Number(r.MATCHUP_MIN) || null,
    }))
    .filter((r) => r.name && r.name !== playerName && Number.isFinite(r.pct))
    .sort((a, b) => b.pct - a.pct);

  if (!rows.length) return null;
  return rows.slice(0, 3);
}

function loadInjuriesToday() {
  const p = path.join(REPO_ROOT, "services", "model", "data", "injuries_today.csv");
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const out: CacheRow[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    const row: CacheRow = {};
    header.forEach((h, idx) => {
      row[h] = parts[idx] ?? "";
    });
    out.push(row);
  }
  return out;
}

function loadTeamContext() {
  const p = path.join(REPO_ROOT, "services", "model", "data", "team_context.csv");
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const out: CacheRow[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    const row: CacheRow = {};
    header.forEach((h, idx) => {
      row[h] = parts[idx] ?? "";
    });
    out.push(row);
  }
  return out;
}

function loadTeamDefenseZones() {
  const p = path.join(REPO_ROOT, "services", "model", "data", "team_defense_zones.csv");
  if (fs.existsSync(p)) {
    const raw = fs.readFileSync(p, "utf-8");
    const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
    if (lines.length < 2) return null;
    const header = parseCsvRow(lines[0]);
    const out: CacheRow[] = [];
    for (let i = 1; i < lines.length; i += 1) {
      const parts = parseCsvRow(lines[i]);
      const row: CacheRow = {};
      header.forEach((h, idx) => {
        row[h] = parts[idx] ?? "";
      });
      out.push(row);
    }
    return out;
  }

  const dataDir = path.join(REPO_ROOT, "services", "model", "data");
  const files = fs
    .readdirSync(dataDir)
    .filter((f) => f.startsWith("zone_stats_team_") && f.endsWith(".json"))
    .map((f) => path.join(dataDir, f))
    .sort((a, b) => fs.statSync(b).mtimeMs - fs.statSync(a).mtimeMs);

  if (!files.length) return null;

  try {
    const payload = JSON.parse(fs.readFileSync(files[0], "utf-8"));
    const tables = payload?.tables || [];
    const target = tables.find((t: any) => (t?.columns || []).includes("TEAM_ABBREVIATION"));
    if (!target) return null;
    const cols: string[] = target.columns || [];
    const rows: any[][] = target.rows || [];
    const idxTeam = cols.indexOf("TEAM_ABBREVIATION");
    const idxZone = cols.indexOf("SHOT_ZONE_BASIC");
    const idxFGM = cols.indexOf("FGM");
    const idxFGA = cols.indexOf("FGA");
    const idxPct = cols.indexOf("FG_PCT");
    if (idxTeam < 0 || idxZone < 0) return null;

    const bucket = (z: string) => {
      if (z === "Restricted Area") return "rim";
      if (z === "In The Paint (Non-RA)") return "paint";
      if (z === "Mid-Range") return "mid";
      if (["Corner 3", "Left Corner 3", "Right Corner 3", "Above the Break 3"].includes(z)) return "three";
      return null;
    };

    const acc: Record<string, { fgm: number; fga: number }> = {};
    const pctFallback: Record<string, number[]> = {};
    for (const row of rows) {
      const team = String(row[idxTeam] || "").toUpperCase();
      const zone = bucket(String(row[idxZone] || ""));
      if (!team || !zone) continue;
      const fgm = idxFGM >= 0 ? Number(row[idxFGM]) : Number.NaN;
      const fga = idxFGA >= 0 ? Number(row[idxFGA]) : Number.NaN;
      const pctRaw = idxPct >= 0 ? Number(row[idxPct]) : Number.NaN;
      const key = `${team}:${zone}`;
      if (!acc[key]) acc[key] = { fgm: 0, fga: 0 };
      if (Number.isFinite(fgm) && Number.isFinite(fga) && fga > 0) {
        acc[key].fgm += fgm;
        acc[key].fga += fga;
      } else if (Number.isFinite(pctRaw)) {
        if (!pctFallback[key]) pctFallback[key] = [];
        pctFallback[key].push(pctRaw);
      }
    }

    const out: CacheRow[] = [];
    Object.keys(acc).forEach((key) => {
      const [team, zone] = key.split(":");
      const entry: CacheRow = { team_abbr: team };
      const stats = acc[key];
      let pct = stats.fga > 0 ? (stats.fgm / stats.fga) * 100 : null;
      if (pct === null || !Number.isFinite(pct)) {
        const list = pctFallback[key] || [];
        if (list.length) {
          const avg = list.reduce((a, b) => a + b, 0) / list.length;
          pct = avg <= 1 ? avg * 100 : avg;
        }
      }
      if (pct !== null && Number.isFinite(pct)) {
        if (zone === "rim") entry.rim_fg_pct_allowed = pct.toFixed(3);
        if (zone === "paint") entry.paint_fg_pct_allowed = pct.toFixed(3);
        if (zone === "mid") entry.mid_fg_pct_allowed = pct.toFixed(3);
        if (zone === "three") entry.three_fg_pct_allowed = pct.toFixed(3);
      }
      out.push(entry);
    });
    return out.length ? out : null;
  } catch {
    return null;
  }
}

function loadPlayerStd(playerName: string) {
  const p = path.join(REPO_ROOT, "services", "model", "data", "player_points_std.csv");
  if (!fs.existsSync(p)) return null;
  const raw = fs.readFileSync(p, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const nameIdx = header.indexOf("player_name");
  const stdIdx = header.indexOf("player_std");
  if (nameIdx < 0 || stdIdx < 0) return null;
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    if (parts[nameIdx] !== playerName) continue;
    const val = Number(parts[stdIdx]);
    if (Number.isFinite(val)) return val;
  }
  return null;
}

function getSgoApiKey() {
  if (process.env.SPORTS_GAME_ODDS_API_KEY) {
    return process.env.SPORTS_GAME_ODDS_API_KEY;
  }
  if (!fs.existsSync(MODEL_ENV_PATH)) return null;
  const raw = fs.readFileSync(MODEL_ENV_PATH, "utf-8");
  const line = raw
    .split(/\r?\n/)
    .map((l) => l.trim())
    .find((l) => l.startsWith("SPORTS_GAME_ODDS_API_KEY="));
  if (!line) return null;
  return line.split("=", 2)[1] || null;
}

function invNorm(p: number) {
  if (!Number.isFinite(p) || p <= 0 || p >= 1) return null;
  const a1 = -39.69683028665376;
  const a2 = 220.9460984245205;
  const a3 = -275.9285104469687;
  const a4 = 138.357751867269;
  const a5 = -30.66479806614716;
  const a6 = 2.506628277459239;
  const b1 = -54.47609879822406;
  const b2 = 161.5858368580409;
  const b3 = -155.6989798598866;
  const b4 = 66.80131188771972;
  const b5 = -13.28068155288572;
  const c1 = -0.007784894002430293;
  const c2 = -0.3223964580411365;
  const c3 = -2.400758277161838;
  const c4 = -2.549732539343734;
  const c5 = 4.374664141464968;
  const c6 = 2.938163982698783;
  const d1 = 0.007784695709041462;
  const d2 = 0.3224671290700398;
  const d3 = 2.445134137142996;
  const d4 = 3.754408661907416;
  const plow = 0.02425;
  const phigh = 1 - plow;
  let q;
  if (p < plow) {
    q = Math.sqrt(-2 * Math.log(p));
    return (((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) /
      ((((d1 * q + d2) * q + d3) * q + d4) * q + 1);
  }
  if (p > phigh) {
    q = Math.sqrt(-2 * Math.log(1 - p));
    return -(((((c1 * q + c2) * q + c3) * q + c4) * q + c5) * q + c6) /
      ((((d1 * q + d2) * q + d3) * q + d4) * q + 1);
  }
  q = p - 0.5;
  const r = q * q;
  return (((((a1 * r + a2) * r + a3) * r + a4) * r + a5) * r + a6) * q /
    (((((b1 * r + b2) * r + b3) * r + b4) * r + b5) * r + 1);
}

function tierFromValue(value: number, values: number[]) {
  if (!Number.isFinite(value) || values.length < 3) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const idx1 = Math.floor(sorted.length / 3);
  const idx2 = Math.floor((sorted.length * 2) / 3);
  const t1 = sorted[idx1];
  const t2 = sorted[idx2];
  if (value <= t1) return "low";
  if (value <= t2) return "mid";
  return "high";
}

function computeSimilarOppFromLogs(
  playerName: string,
  teamAbbr: string,
  oppAbbr: string,
  teamContext: CacheRow[]
) {
  if (!fs.existsSync(LOGS_PATH)) return null;
  const oppRow = teamContext.find((r) => String(r.team_abbr || "").toUpperCase() === oppAbbr.toUpperCase());
  if (!oppRow) return null;
  const oppDef = Number(oppRow.def_rating);
  const oppPace = Number(oppRow.pace);
  if (!Number.isFinite(oppDef) || !Number.isFinite(oppPace)) return null;

  const defValues = teamContext.map((r) => Number(r.def_rating)).filter((n) => Number.isFinite(n));
  const paceValues = teamContext.map((r) => Number(r.pace)).filter((n) => Number.isFinite(n));
  const defTier = tierFromValue(oppDef, defValues);
  const paceTier = tierFromValue(oppPace, paceValues);
  if (!defTier || !paceTier) return null;

  const raw = fs.readFileSync(LOGS_PATH, "utf-8");
  const lines = raw.split(/\r?\n/).filter((l) => l.trim().length > 0);
  if (lines.length < 2) return null;
  const header = parseCsvRow(lines[0]);
  const nameIdx = header.indexOf("player_name");
  const dateIdx = header.indexOf("GAME_DATE");
  const minIdx = header.indexOf("MIN");
  const ptsIdx = header.indexOf("PTS");
  const fgaIdx = header.indexOf("FGA");
  const matchupIdx = header.indexOf("MATCHUP");
  if ([nameIdx, dateIdx, minIdx, ptsIdx, fgaIdx, matchupIdx].some((i) => i < 0)) return null;

  const rows: { date: Date; min: number; pts: number; fga: number; opp: string }[] = [];
  for (let i = 1; i < lines.length; i += 1) {
    const parts = parseCsvRow(lines[i]);
    if (parts[nameIdx] !== playerName) continue;
    const d = parseGameDate(parts[dateIdx] || "");
    if (!d) continue;
    const min = Number(parts[minIdx]);
    const pts = Number(parts[ptsIdx]);
    const fga = Number(parts[fgaIdx]);
    const matchup = parts[matchupIdx] || "";
    const opp = opponentAbbrFromMatchup(matchup, teamAbbr);
    if (!opp) continue;
    if (!Number.isFinite(min) || !Number.isFinite(pts) || !Number.isFinite(fga)) continue;
    if (min < 5) continue;
    rows.push({ date: d, min, pts, fga, opp });
  }
  if (!rows.length) return null;
  rows.sort((a, b) => a.date.getTime() - b.date.getTime());
  const window = 25;
  const lastRows = rows.slice(-window);

  const matches = lastRows.filter((r) => {
    const ctx = teamContext.find((t) => String(t.team_abbr || "").toUpperCase() === r.opp.toUpperCase());
    if (!ctx) return false;
    const d = Number(ctx.def_rating);
    const p = Number(ctx.pace);
    if (!Number.isFinite(d) || !Number.isFinite(p)) return false;
    return tierFromValue(d, defValues) === defTier && tierFromValue(p, paceValues) === paceTier;
  });

  if (!matches.length) return null;
  const avg = (arr: number[]) => arr.reduce((a, b) => a + b, 0) / arr.length;
  const mins = matches.map((r) => r.min);
  const pts = matches.map((r) => r.pts);
  const fga = matches.map((r) => r.fga);
  const asOf = lastRows[lastRows.length - 1].date.toISOString().slice(0, 10);
  return {
    count: matches.length,
    span: lastRows.length,
    pts: avg(pts),
    min: avg(mins),
    fga: avg(fga),
    defTier,
    paceTier,
    asOf,
  };
}

function buildExplanation(
  edge: any,
  cacheRow: CacheRow | null,
  bookStats: { best: number; median: number },
  bookLean: { over: number; under: number },
  lineStats: { median: number; delta: number | null },
  lineValue: number | null,
  modelProj: { points: number | null; std: number | null } | null,
  oppContext: { defRating: number | null; pace: number | null; defRank: number | null; defAvg: number | null } | null,
  zoneDefense: { rim: number | null; paint: number | null; mid: number | null; perimeter: number | null } | null,
  opponentName?: string | null,
  matchupLabel?: string | null,
  oppPosDef?: { fgPct: number | null },
  matchupInfo?: { fgPct: number | null; fg3Pct: number | null; fgaPerMin: number | null },
  primaryDef?: { name: string; fgPct: number | null; fg3Pct: number | null },
  secondaryDef?: { name: string; fgPct: number | null; fg3Pct: number | null },
  defenderStats?: { name: string; fgPct: number | null; fg3Pct: number | null; fga: number | null; min: number | null }[] | null,
  shotMix?: { fg3Rate: number | null },
  availability?: { out: string[]; questionable: string[]; doubtful: string[] },
  last10Override?: { pts: number; min: number; fga: number; asOf: string } | null,
  h2h?: { count: number; pts: number; min: number; fga: number; asOf: string } | null,
  similarOpp?: { count: number; span: number; pts: number; min: number; fga: number; defTier: string; paceTier: string; asOf: string } | null,
  withoutTeammates?: { name: string; count: number; pts: number; min: number; fga: number; asOf: string }[] | null,
  divergence?: { modelEdge: number; implied: number | null; lean: string | null }
) {
  const notes: string[] = [];
  const player = edge.player_name;
  const matchup = opponentName ? `against ${opponentName}` : matchupLabel ? `in ${matchupLabel}` : "in this matchup";
  notes.push(`${player} lines up ${matchup} today.`);
  notes.push(`Model says P=${Number(edge.p).toFixed(3)} with EV=${Number(edge.ev).toFixed(3)}.`);

  if (oppContext && (oppContext.defRating || oppContext.pace)) {
    const parts: string[] = [];
    if (oppContext.defRating) {
      let rankText = "";
      if (oppContext.defRank && oppContext.defAvg) {
        rankText = ` (league avg ${oppContext.defAvg.toFixed(1)}, rank ${oppContext.defRank}/30)`;
      }
      parts.push(`defensive rating ${oppContext.defRating.toFixed(1)}${rankText}`);
    }
    if (oppContext.pace) parts.push(`pace ${oppContext.pace.toFixed(1)}`);
    if (parts.length) notes.push(`Opponent profile: ${parts.join(", ")}.`);
  }

  if (zoneDefense) {
    const parts: string[] = [];
    if (zoneDefense.perimeter) parts.push(`perimeter FG% allowed ${zoneDefense.perimeter.toFixed(1)}`);
    if (zoneDefense.paint) parts.push(`paint FG% allowed ${zoneDefense.paint.toFixed(1)}`);
    if (zoneDefense.rim) parts.push(`rim FG% allowed ${zoneDefense.rim.toFixed(1)}`);
    if (zoneDefense.mid) parts.push(`mid-range FG% allowed ${zoneDefense.mid.toFixed(1)}`);
    if (parts.length) notes.push(`Zone defense snapshot: ${parts.join(", ")}.`);
  }

  if (Number.isFinite(bookStats.best) && Number.isFinite(bookStats.median)) {
    const diff = bookStats.best - bookStats.median;
    if (Math.abs(diff) >= 0.05) {
      notes.push(`This book is ${diff > 0 ? "better" : "worse"} than the median by ${diff.toFixed(2)} in decimal odds.`);
    } else {
      notes.push("Odds are basically in line with the market median.");
    }
  }

  if (Number.isFinite(lineStats.median) && lineStats.delta !== null && Math.abs(lineStats.delta) >= 1) {
    notes.push(`This line is ${lineStats.delta > 0 ? "above" : "below"} the market median by ${Math.abs(lineStats.delta).toFixed(1)}.`);
  }

  if (bookLean.over + bookLean.under > 0) {
    const lean = bookLean.over >= bookLean.under ? "over" : "under";
    const side = String(edge.side || "").toLowerCase();
    let line = `Books lean ${lean} (${bookLean.over}-${bookLean.under}).`;
    if (side && side !== lean) {
      line += ` Model still prefers ${side}.`;
    }
    notes.push(line);
  }

  if (matchupInfo && (matchupInfo.fgPct || matchupInfo.fg3Pct || matchupInfo.fgaPerMin)) {
    const parts: string[] = [];
    const fg = formatPercent(matchupInfo.fgPct);
    const fg3 = formatPercent(matchupInfo.fg3Pct);
    if (fg !== null) parts.push(`FG% ${fg.toFixed(1)}`);
    if (fg3 !== null) parts.push(`3P% ${fg3.toFixed(1)}`);
    if (matchupInfo.fgaPerMin) parts.push(`FGA/min ${matchupInfo.fgaPerMin.toFixed(2)}`);
    if (parts.length) notes.push(`Matchup profile vs similar defenders: ${parts.join(", ")}.`);
  }

  if (shotMix && shotMix.fg3Rate !== null) {
    const label = shotMix.fg3Rate >= 35 ? "perimeter-leaning" : "balanced";
    notes.push(`Shot mix is ${label}: ${shotMix.fg3Rate.toFixed(1)}% from 3 (matchup sample).`);
    const side = String(edge.side || "").toLowerCase();
    if (side === "under" && zoneDefense?.perimeter && shotMix.fg3Rate >= 35) {
      notes.push(
        `With a perimeter-heavy profile and opponent perimeter FG% allowed at ${zoneDefense.perimeter.toFixed(1)}, ` +
        `the under has additional matchup support.`
      );
    }
  }

  if (oppPosDef && oppPosDef.fgPct) {
    const fg = formatPercent(oppPosDef.fgPct);
    if (fg !== null) {
      notes.push(`Opponent allows ~${fg.toFixed(1)}% FG to this position.`);
    }
  }

  if (last10Override && Number.isFinite(last10Override.pts)) {
    const asOf = last10Override.asOf ? ` as of ${last10Override.asOf}` : "";
    notes.push(`Recent form: ${last10Override.pts.toFixed(1)} pts (last 10 avg${asOf}).`);
    notes.push(`Minutes trend: ${last10Override.min.toFixed(1)} min (last 10 avg${asOf}).`);
    notes.push(`Shot volume: ${last10Override.fga.toFixed(1)} FGA (last 10 avg${asOf}).`);
  } else if (cacheRow) {
    const paceDiff = Number(cacheRow.pace_diff || 0);
    const defDiff = Number(cacheRow.def_diff || 0);
    const usage = Number(cacheRow.usage_proxy || 0);
    const outCount = Number(cacheRow.teammate_out_count || 0);
    const starterOut = Number(cacheRow.starter_out_count || 0);
  const pts10 = Number(cacheRow.display_pts_last10 || cacheRow.pts_ma_10 || 0);
  const min10 = Number(cacheRow.display_min_last10 || cacheRow.min_ma_10 || 0);
  const fga10 = Number(cacheRow.display_fga_last10 || cacheRow.fga_ma_10 || 0);
    const oppDef = Number(cacheRow.opp_def_rating || 0);
    const teamOff = Number(cacheRow.team_off_rating || 0);

    const asOf = cacheRow?.GAME_DATE ? ` as of ${cacheRow.GAME_DATE}` : "";
    if (Number.isFinite(pts10) && pts10 > 0) notes.push(`Recent form: ${pts10.toFixed(1)} pts (last 10 avg${asOf}).`);
    if (Number.isFinite(min10) && min10 > 0) notes.push(`Minutes trend: ${min10.toFixed(1)} min (last 10 avg${asOf}).`);
    if (Number.isFinite(fga10) && fga10 > 0) notes.push(`Shot volume: ${fga10.toFixed(1)} FGA (last 10 avg${asOf}).`);
    if (Number.isFinite(oppDef) && Number.isFinite(teamOff)) {
      notes.push(`Opponent DEF rating ${oppDef.toFixed(1)} vs team OFF ${teamOff.toFixed(1)}.`);
    }
    if (usage >= 0.4) notes.push("Usage proxy is elevated, indicating high involvement.");
    if (paceDiff >= 1.5) notes.push("Matchup pace is faster than team average.");
    if (paceDiff <= -1.5) notes.push("Matchup pace is slower than team average.");
    if (defDiff <= -2) notes.push("Opponent defense grades easier than this offense.");
    if (defDiff >= 2) notes.push("Opponent defense grades tougher than this offense.");
    if (outCount >= 2 || starterOut >= 1) notes.push("Team injuries may elevate usage for remaining starters.");
  }

  if (primaryDef) {
    const fg = formatPercent(primaryDef.fgPct);
    const fg3 = formatPercent(primaryDef.fg3Pct);
    const parts = [
      fg !== null ? `FG% ${fg.toFixed(1)}` : null,
      fg3 !== null ? `3P% ${fg3.toFixed(1)}` : null,
    ].filter(Boolean);
    notes.push(`Primary defender: ${primaryDef.name}${parts.length ? ` (${parts.join(", ")})` : ""}.`);
  }
  if (secondaryDef && secondaryDef.name !== primaryDef?.name) {
    const fg = formatPercent(secondaryDef.fgPct);
    const fg3 = formatPercent(secondaryDef.fg3Pct);
    const parts = [
      fg !== null ? `FG% ${fg.toFixed(1)}` : null,
      fg3 !== null ? `3P% ${fg3.toFixed(1)}` : null,
    ].filter(Boolean);
    notes.push(`Secondary defender: ${secondaryDef.name}${parts.length ? ` (${parts.join(", ")})` : ""}.`);
  }

  if (defenderStats && defenderStats.length) {
    const lines = defenderStats.map((d) => {
      const fg = formatPercent(d.fgPct);
      const fg3 = formatPercent(d.fg3Pct);
      const parts = [
        fg !== null ? `FG% ${fg.toFixed(1)}` : null,
        fg3 !== null ? `3P% ${fg3.toFixed(1)}` : null,
        Number.isFinite(d.fga) ? `FGA ${d.fga?.toFixed(1)}` : null,
        Number.isFinite(d.min) ? `MIN ${d.min?.toFixed(1)}` : null,
      ].filter(Boolean);
      return `${d.name}${parts.length ? ` (${parts.join(", ")})` : ""}`;
    });
    if (lines.length) {
      notes.push(`Top matchup defenders vs this player: ${lines.join("; ")}.`);
    }
  }

  if (h2h && h2h.count > 0) {
    notes.push(
      `Previous matchups vs this opponent: ${h2h.pts.toFixed(1)} pts, ${h2h.min.toFixed(1)} min, ` +
      `${h2h.fga.toFixed(1)} FGA (last ${h2h.count}, as of ${h2h.asOf}).`
    );
  }

  if (similarOpp && similarOpp.count > 0) {
    notes.push(
      `Similar opponents (DEF ${similarOpp.defTier}, pace ${similarOpp.paceTier}) in last ${similarOpp.span}: ` +
      `${similarOpp.pts.toFixed(1)} pts, ${similarOpp.min.toFixed(1)} min, ${similarOpp.fga.toFixed(1)} FGA ` +
      `(n=${similarOpp.count}, as of ${similarOpp.asOf}).`
    );
  }

  if (withoutTeammates && withoutTeammates.length) {
    withoutTeammates.forEach((t) => {
      notes.push(
        `Without ${t.name}: ${t.pts.toFixed(1)} pts in ${t.min.toFixed(1)} min ` +
        `(last ${t.count}, as of ${t.asOf}).`
      );
    });
  }

  if (modelProj && modelProj.points !== null) {
    const rounded = Math.round(modelProj.points);
    notes.push(`Model projects about ${modelProj.points.toFixed(1)} points (≈${rounded} rounded).`);
    if (lineValue !== null && modelProj.points < lineValue - 2) {
      notes.push(
        `That projection sits well below the line (${lineValue.toFixed(1)}), which usually means the model is pricing a low-usage or low-efficiency game at this spot.`
      );
      const recentPts = last10Override?.pts ?? Number(cacheRow?.display_pts_last10 || cacheRow?.pts_ma_10 || 0);
      if (Number.isFinite(recentPts) && recentPts > lineValue) {
        notes.push(
          `Even if recent averages are above the line, the model is leaning under here because the probability curve implies a lower central outcome given variance.`
        );
      }
    }
  }

  if (divergence && divergence.lean) {
    const implied = divergence.implied;
    const diff = divergence.modelEdge;
    if (Math.abs(diff) >= 0.05) {
      const impliedText = implied !== null ? ` vs implied ${implied.toFixed(3)}` : "";
      const support: string[] = [];
      if (lineValue && Number.isFinite(lineValue)) {
        const recentPts = last10Override?.pts ?? Number(cacheRow?.display_pts_last10 || cacheRow?.pts_ma_10 || 0);
        if (Number.isFinite(recentPts) && recentPts > 0) {
          const delta = recentPts - lineValue;
          if (Math.abs(delta) >= 0.5) {
            support.push(`recent form is ${Math.abs(delta).toFixed(1)} ${delta > 0 ? "above" : "below"} the line`);
          }
        }
        if (h2h && Number.isFinite(h2h.pts)) {
          const delta = h2h.pts - lineValue;
          if (Math.abs(delta) >= 0.5) {
            support.push(`H2H avg is ${Math.abs(delta).toFixed(1)} ${delta > 0 ? "above" : "below"} the line`);
          }
        }
        if (similarOpp && Number.isFinite(similarOpp.pts)) {
          const delta = similarOpp.pts - lineValue;
          if (Math.abs(delta) >= 0.5) {
            support.push(`similar-opponent avg is ${Math.abs(delta).toFixed(1)} ${delta > 0 ? "above" : "below"} the line`);
          }
        }
      }
      const supportText = support.length ? ` Quick why: ${support.join("; ")}.` : "";
      notes.push(
        `Model diverges from the market lean (${divergence.lean}) because its win probability ` +
        `is ${(diff > 0 ? "higher" : "lower")} by ${Math.abs(diff).toFixed(3)}${impliedText}.${supportText}`
      );
      if (implied !== null) {
        notes.push(
          `Implied probability is just the market's break-even chance based on the odds; the model's P is its own estimated win chance.`
        );
      }
    }
  }

  if (availability) {
    const { out, questionable, doubtful } = availability;
    if (out.length) notes.push(`Out: ${out.join(", ")}.`);
    if (questionable.length) notes.push(`Questionable: ${questionable.join(", ")}.`);
    if (doubtful.length) notes.push(`Doubtful: ${doubtful.join(", ")}.`);
    if (out.length && cacheRow) {
      const usage = Number(cacheRow.usage_proxy || 0);
      if (Number.isFinite(usage) && usage > 0) {
        notes.push(`With key teammates out, usage proxy sits at ${usage.toFixed(2)} (higher means more on-ball reps).`);
      }
    }
  }

  return notes.join(" ");
}

export async function GET(req: Request) {
  try {
    const { searchParams } = new URL(req.url);
    const id = searchParams.get("id");
    if (!id) {
      return NextResponse.json({ explanation: "Missing id." }, { status: 400 });
    }

    const sb = supabaseAdmin();
    const { data: edge, error } = await sb.from("edges").select("*").eq("id", id).single();
    if (error || !edge) {
      return NextResponse.json({ explanation: "Edge not found." }, { status: 404 });
    }

    const { data: peers } = await sb
      .from("edges")
      .select("odds,line,side,book")
      .eq("event_id", edge.event_id)
      .eq("market", edge.market)
      .eq("player_name", edge.player_name);

    const oddsList = (peers || [])
      .filter((p: any) => p.side === edge.side && Number(p.line) === Number(edge.line))
      .map((p: any) => Number(p.odds))
      .filter((n: number) => Number.isFinite(n));
    oddsList.sort((a: number, b: number) => a - b);
    const best = oddsList.length ? oddsList[oddsList.length - 1] : Number.NaN;
    const median = oddsList.length ? oddsList[Math.floor(oddsList.length / 2)] : Number.NaN;

    const lineList = (peers || [])
      .filter((p: any) => p.side === edge.side)
      .map((p: any) => Number(p.line))
      .filter((n: number) => Number.isFinite(n));
    lineList.sort((a: number, b: number) => a - b);
    const lineMedian = lineList.length ? lineList[Math.floor(lineList.length / 2)] : Number.NaN;
    const lineDelta = Number.isFinite(lineMedian) ? Number(edge.line) - lineMedian : null;

    const cacheRow = edge.market === "player_points" ? parseFeatureCacheRow(edge.player_name) : null;

    const bookPairs: Record<string, { over?: number; under?: number }> = {};
    for (const p of peers || []) {
      const book = String(p.book || "").toLowerCase();
      if (!book) continue;
      if (!bookPairs[book]) bookPairs[book] = {};
      if (p.side === "over") bookPairs[book].over = Number(p.odds);
      if (p.side === "under") bookPairs[book].under = Number(p.odds);
    }
    let overLean = 0;
    let underLean = 0;
    Object.values(bookPairs).forEach((pair) => {
      if (!pair.over || !pair.under) return;
      const po = impliedProbFromDecimal(pair.over);
      const pu = impliedProbFromDecimal(pair.under);
      if (po === null || pu === null) return;
      if (po > pu) overLean += 1;
      if (pu > po) underLean += 1;
    });

    let opponentName: string | null = null;
    let matchupLabel: string | null = null;
    let oppPosDef: { fgPct: number | null } | undefined;
    let matchupInfo: { fgPct: number | null; fg3Pct: number | null; fgaPerMin: number | null } | undefined;
    let primaryDef: { name: string; fgPct: number | null; fg3Pct: number | null } | undefined;
    let secondaryDef: { name: string; fgPct: number | null; fg3Pct: number | null } | undefined;
    let shotMix: { fg3Rate: number | null } | undefined;
    let availability: { out: string[]; questionable: string[]; doubtful: string[] } | undefined;
    let last10Override: { pts: number; min: number; fga: number; asOf: string } | null = null;
    let h2h: { count: number; pts: number; min: number; fga: number; asOf: string } | null = null;
    let defenderStats:
      | { name: string; fgPct: number | null; fg3Pct: number | null; fga: number | null; min: number | null }[]
      | null = null;
    let similarOpp:
      | { count: number; span: number; pts: number; min: number; fga: number; defTier: string; paceTier: string; asOf: string }
      | null = null;
    let withoutTeammates:
      | { name: string; count: number; pts: number; min: number; fga: number; asOf: string }[]
      | null = null;
    if (edge.market === "player_points") {
      const apiKey = getSgoApiKey();
      if (apiKey) {
        const url = new URL("https://api.sportsgameodds.com/v2/events/");
        url.searchParams.set("leagueID", "NBA");
        url.searchParams.set("eventID", String(edge.event_id));
        url.searchParams.set("eventId", String(edge.event_id));
        const res = await fetch(url.toString(), { headers: { "x-api-key": apiKey } });
        if (res.ok) {
          const payload = await res.json();
          const events = payload?.data || payload?.events || [];
          const eventList = Array.isArray(events) ? events : [events];
          const event = eventList.find((e: any) => {
            const id = String(e?.id ?? e?.eventID ?? "");
            return id === String(edge.event_id);
          });
          if (event) {
            const teamAbbr = cacheRow?.team_abbr;
            opponentName = getOpponentName(event, teamAbbr);
            matchupLabel = getMatchupLabel(event);
            if (opponentName && teamAbbr && TEAM_ABBR_TO_NAME[teamAbbr] === opponentName) {
              opponentName = null;
            }
            const oppAbbr = opponentName ? TEAM_NAME_TO_ABBR[opponentName] : null;
            if (oppAbbr) {
              const defenderRows = loadDefenderRatings();
              if (defenderRows && cacheRow?.team_abbr) {
                const playerPos = defenderRows.find((r) => r.player_name === edge.player_name)?.player_position;
                if (playerPos) {
                  const pos = String(playerPos).startsWith("G") ? "G" : String(playerPos).startsWith("F") ? "F" : "C";
                  const vals = defenderRows
                    .filter((r) => r.team_abbr === oppAbbr && r.player_position?.startsWith(pos))
                    .map((r) => Number(r.def_fg_pct))
                    .filter((n) => Number.isFinite(n));
                  if (vals.length) {
                    const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
                    oppPosDef = { fgPct: avg };
                  }
                }
              }
            }
          }
        }
      }

      const injuries = loadInjuriesToday();
      if (injuries && cacheRow?.team_abbr) {
        const team = String(cacheRow.team_abbr || "").toUpperCase();
        const teamRows = injuries.filter((r) => String(r.team_abbr || "").toUpperCase() === team);
        const out = teamRows
          .filter((r) => String(r.status || "").toUpperCase() === "OUT")
          .map((r) => String(r.player_name || "").trim())
          .filter(Boolean);
        const questionable = teamRows
          .filter((r) => String(r.status || "").toUpperCase().startsWith("Q"))
          .map((r) => String(r.player_name || "").trim())
          .filter(Boolean);
        const doubtful = teamRows
          .filter((r) => String(r.status || "").toUpperCase().startsWith("D"))
          .map((r) => String(r.player_name || "").trim())
          .filter(Boolean);
        if (out.length || questionable.length || doubtful.length) {
          availability = { out, questionable, doubtful };
        }
      }

      const offenseRows = loadOffenseMatchups();
      if (offenseRows) {
        const row = offenseRows.find((r) => r.OFF_PLAYER_NAME === edge.player_name);
        if (row) {
          const fga = Number(row.MATCHUP_FGA);
          const fg3a = Number(row.MATCHUP_FG3A);
          const min = Number(row.MATCHUP_MIN);
          matchupInfo = {
            fgPct: Number(row.MATCHUP_FG_PCT) || null,
            fg3Pct: Number(row.MATCHUP_FG3_PCT) || null,
            fgaPerMin: fga && min ? fga / min : null,
          };
          if (fg3a && fga) {
            shotMix = { fg3Rate: (fg3a / fga) * 100 };
          }
        }
      }

      const defRows = loadDefenseMatchups();
      if (defRows) {
        const rows = defRows
          .filter((r) => r.FOCUS_PLAYER_NAME === edge.player_name)
          .map((r) => ({
            name: String(r.DEF_PLAYER_NAME || "").trim(),
            pct: Number(r.PERCENT_OF_TIME),
            fgPct: Number(r.MATCHUP_FG_PCT) || null,
            fg3Pct: Number(r.MATCHUP_FG3_PCT) || null,
          }))
          .filter((r) => r.name && r.name !== edge.player_name && Number.isFinite(r.pct))
          .sort((a, b) => b.pct - a.pct);
        if (rows.length > 0) {
          primaryDef = { name: rows[0].name, fgPct: rows[0].fgPct, fg3Pct: rows[0].fg3Pct };
        }
        if (rows.length > 1) {
          const next = rows.find((r) => r.name !== rows[0].name);
          if (next) {
            secondaryDef = { name: next.name, fgPct: next.fgPct, fg3Pct: next.fg3Pct };
          }
        }
      }

      try {
        last10Override = await fetchEspnLast10(edge.player_name, cacheRow?.team_abbr, edge.starts_at || null);
      } catch {
        last10Override = null;
      }
      if (!last10Override) {
        last10Override = computeLast10FromLogs(edge.player_name);
      }
    }

    const oppAbbr = opponentName ? TEAM_NAME_TO_ABBR[opponentName] : null;
    let oppContext:
      | { defRating: number | null; pace: number | null; defRank: number | null; defAvg: number | null }
      | null = null;
    let zoneDefense:
      | { rim: number | null; paint: number | null; mid: number | null; perimeter: number | null }
      | null = null;
    if (oppAbbr) {
      h2h = computeH2HFromLogs(edge.player_name, oppAbbr);
      defenderStats = loadDefenseMatchupStats(edge.player_name, oppAbbr);
      if (cacheRow?.team_abbr) {
        const teamContext = loadTeamContext();
        if (teamContext) {
          similarOpp = computeSimilarOppFromLogs(edge.player_name, cacheRow.team_abbr, oppAbbr, teamContext);
          const ctxRow = teamContext.find(
            (r) => String(r.team_abbr || "").toUpperCase() === oppAbbr.toUpperCase()
          );
          if (ctxRow) {
            const defRating = Number(ctxRow.def_rating);
            const pace = Number(ctxRow.pace);
            const defValues = teamContext
              .map((r) => Number(r.def_rating))
              .filter((n) => Number.isFinite(n))
              .sort((a, b) => a - b);
            const defAvg =
              defValues.length > 0 ? defValues.reduce((a, b) => a + b, 0) / defValues.length : null;
            let defRank: number | null = null;
            if (Number.isFinite(defRating) && defValues.length) {
              const idx = defValues.findIndex((v) => v >= defRating);
              defRank = idx >= 0 ? idx + 1 : defValues.length;
            }
            oppContext = {
              defRating: Number.isFinite(defRating) ? defRating : null,
              pace: Number.isFinite(pace) ? pace : null,
              defRank,
              defAvg,
            };
          }
        }
        const zoneRows = loadTeamDefenseZones();
        if (zoneRows) {
          const zr = zoneRows.find((r) => String(r.team_abbr || "").toUpperCase() === oppAbbr.toUpperCase());
          if (zr) {
            const rim = Number(zr.rim_fg_pct_allowed);
            const paint = Number(zr.paint_fg_pct_allowed);
            const mid = Number(zr.mid_fg_pct_allowed);
            const perimeter = Number(zr.three_fg_pct_allowed);
            zoneDefense = {
              rim: Number.isFinite(rim) ? rim : null,
              paint: Number.isFinite(paint) ? paint : null,
              mid: Number.isFinite(mid) ? mid : null,
              perimeter: Number.isFinite(perimeter) ? perimeter : null,
            };
          }
        }
        if (!zoneDefense && cacheRow) {
          const rim = Number(cacheRow.opp_zone_rim_fg_pct);
          const paint = Number(cacheRow.opp_zone_paint_fg_pct);
          const mid = Number(cacheRow.opp_zone_mid_fg_pct);
          const perimeter = Number(cacheRow.opp_zone_three_fg_pct);
          if (
            [rim, paint, mid, perimeter].some((n) => Number.isFinite(n) && n > 0)
          ) {
            zoneDefense = {
              rim: Number.isFinite(rim) ? rim : null,
              paint: Number.isFinite(paint) ? paint : null,
              mid: Number.isFinite(mid) ? mid : null,
              perimeter: Number.isFinite(perimeter) ? perimeter : null,
            };
          }
        }
      }
    }

    const implied = Number.isFinite(median) ? impliedProbFromDecimal(median) : null;
    const side = String(edge.side || "").toLowerCase();
    const lean = overLean === underLean ? null : overLean > underLean ? "over" : "under";
    const modelEdge = Number(edge.p) - (implied ?? Number(edge.p));
    const lineValue = Number.isFinite(Number(edge.line)) ? Number(edge.line) : null;
    let modelProj: { points: number | null; std: number | null } | null = null;
    if (lineValue !== null) {
      const std = loadPlayerStd(edge.player_name);
      if (std && Number.isFinite(std) && std > 0) {
        const p = Number(edge.p);
        const cdfProb = side === "over" ? 1 - p : p;
        const z = invNorm(cdfProb);
        if (z !== null) {
          modelProj = { points: lineValue - z * std, std };
        }
      }
    }
    if (availability?.out?.length && cacheRow?.team_abbr) {
      withoutTeammates = computeWithoutTeammatesFromLogs(
        edge.player_name,
        cacheRow.team_abbr,
        availability.out
      );
    }

    const explanation = buildExplanation(
      edge,
      cacheRow,
      { best, median },
      { over: overLean, under: underLean },
      { median: lineMedian, delta: lineDelta },
      lineValue,
      modelProj,
      oppContext,
      zoneDefense,
      opponentName,
      matchupLabel,
      oppPosDef,
      matchupInfo,
      primaryDef,
      secondaryDef,
      defenderStats,
      shotMix,
      availability,
      last10Override,
      h2h,
      similarOpp,
      withoutTeammates,
      { modelEdge, implied, lean: lean && side && side !== lean ? lean : null }
    );

    return NextResponse.json({ explanation });
  } catch (e: any) {
    return NextResponse.json({ explanation: e?.message ?? "Unknown error" }, { status: 500 });
  }
}
