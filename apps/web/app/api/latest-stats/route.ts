import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";
import readline from "readline";

const REPO_ROOT = path.resolve(process.cwd(), "..", "..");
const LOGS_PATH = path.join(REPO_ROOT, "services", "model", "data", "nba_player_logs_points_all.csv");

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

export async function GET() {
  try {
    if (!fs.existsSync(LOGS_PATH)) {
      return NextResponse.json({ latest_game_date: null, error: "Missing logs file." }, { status: 404 });
    }

    const stream = fs.createReadStream(LOGS_PATH, { encoding: "utf-8" });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });

    let header: string[] | null = null;
    let gameDateIdx = -1;
    let latest: Date | null = null;

    for await (const line of rl) {
      if (!line.trim()) continue;
      if (!header) {
        header = parseCsvRow(line);
        gameDateIdx = header.indexOf("GAME_DATE");
        continue;
      }
      if (gameDateIdx < 0) continue;
      const parts = parseCsvRow(line);
      const raw = parts[gameDateIdx] || "";
      const d = parseGameDate(raw);
      if (!d) continue;
      if (!latest || d > latest) latest = d;
    }

    const latestStr = latest ? latest.toISOString().slice(0, 10) : null;
    return NextResponse.json({ latest_game_date: latestStr, error: null });
  } catch (e: any) {
    return NextResponse.json({ latest_game_date: null, error: e?.message ?? "Unknown error" }, { status: 500 });
  }
}
