create table if not exists predictions (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  provider text,
  event_id text,
  sport text,
  market text,
  player_name text,
  side text,
  line numeric,
  book text,
  odds numeric,
  p_model numeric,
  p_raw numeric,
  source text,
  model_version text,
  starts_at timestamptz,
  game_date date,
  actual_value numeric,
  hit boolean
);

create unique index if not exists predictions_unique_idx
  on predictions (provider, event_id, market, player_name, side, line, book);
