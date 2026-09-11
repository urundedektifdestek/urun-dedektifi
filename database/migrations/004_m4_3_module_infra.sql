
CREATE TABLE IF NOT EXISTS local_production_quotes (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  product_id TEXT,
  product_title TEXT,
  production_type TEXT,
  supplier_name TEXT,
  city TEXT,
  sample_cost NUMERIC,
  unit_cost NUMERIC,
  moq INTEGER,
  lead_time TEXT,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_local_quotes_user_created ON local_production_quotes(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS cost_calculations (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  product_id TEXT,
  product_title TEXT,
  sale_price NUMERIC,
  unit_cost NUMERIC,
  commission_pct NUMERIC,
  shipping_packaging NUMERIC,
  ads_cost NUMERIC,
  tax_cost NUMERIC,
  return_reserve NUMERIC,
  net_profit NUMERIC,
  net_margin NUMERIC,
  roi NUMERIC,
  locked BOOLEAN NOT NULL DEFAULT TRUE,
  missing_fields JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_cost_user_created ON cost_calculations(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ad_signals (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  product_id TEXT,
  product_title TEXT,
  platform TEXT NOT NULL DEFAULT 'Meta Ads',
  creative_count INTEGER,
  active_pages INTEGER,
  signal_label TEXT,
  confidence NUMERIC,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_ad_signals_user_created ON ad_signals(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS saas_accounts (
  user_id TEXT PRIMARY KEY,
  plan TEXT NOT NULL DEFAULT 'builder_beta',
  daily_scan_limit INTEGER NOT NULL DEFAULT 30,
  ai_analysis_limit INTEGER NOT NULL DEFAULT 100,
  saved_product_limit INTEGER NOT NULL DEFAULT 500,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  settings JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS owner_events (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  event_type TEXT NOT NULL,
  title TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_owner_events_created ON owner_events(created_at DESC);

