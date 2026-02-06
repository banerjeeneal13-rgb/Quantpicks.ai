create table if not exists live_player_stats (
  id uuid primary key default gen_random_uuid(),
  updated_at timestamptz default now(),
  game_id text,
  game_date date,
  team_abbr text,
  player_name text,
  minutes text,
  pts numeric,
  reb numeric,
  ast numeric,
  fg3m numeric,
  blk numeric,
  stl numeric,
  tov numeric
);

create unique index if not exists live_player_stats_unique_idx
  on live_player_stats (game_id, player_name);
