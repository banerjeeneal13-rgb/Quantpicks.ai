create table if not exists bets (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  source text,
  player_name text,
  market text,
  side text,
  line numeric,
  odds numeric,
  stake numeric,
  unit numeric,
  book text,
  starts_at timestamptz,
  result text,
  profit numeric,
  notes text
);
