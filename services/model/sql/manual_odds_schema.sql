create table if not exists manual_odds (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  pulled_at timestamptz,
  game_date date,
  game text,
  event_id text,
  player_name text,
  market text,
  line numeric,
  over_odds numeric,
  under_odds numeric,
  book text,
  source text default 'manual',
  notes text
);

create unique index if not exists manual_odds_unique_idx
  on manual_odds (player_name, market, line, book, pulled_at);
