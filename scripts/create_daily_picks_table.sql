-- Create the daily_picks table for snapshotting daily picks.
-- Run this in the Supabase SQL Editor.

CREATE TABLE IF NOT EXISTS daily_picks (
  id            UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  pick_date     DATE NOT NULL UNIQUE,
  top3_picks    JSONB DEFAULT '[]'::jsonb,
  premium_picks JSONB DEFAULT '[]'::jsonb,
  all_picks     JSONB DEFAULT '[]'::jsonb,
  top3_count    INTEGER DEFAULT 0,
  premium_count INTEGER DEFAULT 0,
  created_at    TIMESTAMPTZ DEFAULT now(),
  updated_at    TIMESTAMPTZ DEFAULT now()
);

-- Index on date for fast lookups
CREATE INDEX IF NOT EXISTS idx_daily_picks_date ON daily_picks(pick_date);

-- Enable RLS (optional — disable if using service role key only)
ALTER TABLE daily_picks ENABLE ROW LEVEL SECURITY;

-- Allow service role full access
CREATE POLICY "Service role full access"
  ON daily_picks
  FOR ALL
  USING (true)
  WITH CHECK (true);

-- Add updated_at trigger
CREATE OR REPLACE FUNCTION update_daily_picks_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_daily_picks_updated_at ON daily_picks;
CREATE TRIGGER trigger_daily_picks_updated_at
  BEFORE UPDATE ON daily_picks
  FOR EACH ROW
  EXECUTE FUNCTION update_daily_picks_updated_at();
