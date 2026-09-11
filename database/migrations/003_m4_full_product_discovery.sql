

CREATE TABLE IF NOT EXISTS user_profiles (
  user_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  budget NUMERIC,
  min_margin NUMERIC,
  max_sellers INTEGER,
  min_rating NUMERIC,
  risk_level TEXT,
  sourcing_preference TEXT,
  categories JSONB NOT NULL DEFAULT '[]'::jsonb,
  preferences JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS source_registry (
  source TEXT PRIMARY KEY,
  status TEXT NOT NULL DEFAULT 'configured',
  health TEXT NOT NULL DEFAULT 'unknown',
  last_checked_at TIMESTAMPTZ,
  last_error TEXT,
  config JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS scan_runs (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  source TEXT NOT NULL,
  query TEXT,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  found_count INTEGER NOT NULL DEFAULT 0,
  saved_count INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  params JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_scan_runs_user_started ON scan_runs(user_id, started_at DESC);

CREATE TABLE IF NOT EXISTS discovered_products (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  source TEXT NOT NULL,
  product_url TEXT,
  title TEXT,
  brand TEXT,
  seller TEXT,
  image TEXT,
  first_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  current_price NUMERIC,
  rating_value NUMERIC,
  review_count INTEGER,
  visible_sales_signal TEXT,
  exact_sales_count INTEGER,
  score INTEGER NOT NULL DEFAULT 0,
  decision TEXT,
  momentum_label TEXT,
  categories JSONB NOT NULL DEFAULT '[]'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  ai_council JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_discovered_user_last ON discovered_products(user_id, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_discovered_score ON discovered_products(score DESC);
CREATE INDEX IF NOT EXISTS idx_discovered_source ON discovered_products(source);

CREATE TABLE IF NOT EXISTS product_snapshots (
  id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL,
  user_id TEXT NOT NULL DEFAULT 'demo',
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  price NUMERIC,
  rating_value NUMERIC,
  review_count INTEGER,
  seller TEXT,
  image TEXT,
  raw JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_snapshots_product_created ON product_snapshots(product_id, created_at DESC);

CREATE TABLE IF NOT EXISTS momentum_events (
  id TEXT PRIMARY KEY,
  product_id TEXT NOT NULL,
  user_id TEXT NOT NULL DEFAULT 'demo',
  source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  event_type TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT 'info',
  message TEXT,
  delta JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_momentum_user_created ON momentum_events(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS alerts (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  product_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  alert_type TEXT NOT NULL,
  severity TEXT NOT NULL DEFAULT 'info',
  title TEXT,
  message TEXT,
  seen BOOLEAN NOT NULL DEFAULT FALSE,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS idx_alerts_user_created ON alerts(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS notification_outbox (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  channel TEXT NOT NULL DEFAULT 'in_app',
  status TEXT NOT NULL DEFAULT 'pending',
  title TEXT,
  body TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS sourcing_offers (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  product_id TEXT,
  source TEXT,
  supplier_name TEXT,
  unit_price NUMERIC,
  currency TEXT,
  moq INTEGER,
  lead_time TEXT,
  oem BOOLEAN,
  private_label BOOLEAN,
  confidence NUMERIC,
  url TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw JSONB NOT NULL DEFAULT '{}'::jsonb
);

INSERT INTO source_registry(source,status,health,config) VALUES
  ('Trendyol','configured','ready','{"mode":"public_search_and_product_page"}'::jsonb),
  ('Shopify','adapter_ready','needs_targets','{"requires":"shopify_store_urls_or_search_provider"}'::jsonb),
  ('Meta Ads','adapter_ready','needs_provider','{"requires":"Meta Ad Library/API or browser worker"}'::jsonb),
  ('Alibaba','adapter_ready','needs_provider','{"requires":"supplier search provider/API/manual quote"}'::jsonb),
  ('Yerli Üretim','adapter_ready','needs_provider','{"requires":"supplier directory or manual offers"}'::jsonb)
ON CONFLICT (source) DO NOTHING;

