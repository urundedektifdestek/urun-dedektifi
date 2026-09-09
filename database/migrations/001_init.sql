CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS analyses (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  message TEXT,
  product_url TEXT,
  product_text TEXT,
  source TEXT,
  score INTEGER,
  decision TEXT,
  ai_council JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_gate JSONB NOT NULL DEFAULT '{}'::jsonb,
  products JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_analyses_user_created ON analyses(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_analyses_source_created ON analyses(source, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_analyses_score ON analyses(score DESC);

CREATE TABLE IF NOT EXISTS saved_products (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  analysis_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  title TEXT NOT NULL,
  source TEXT,
  product_url TEXT,
  score INTEGER,
  decision TEXT,
  product JSONB NOT NULL DEFAULT '{}'::jsonb,
  notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_saved_user_created ON saved_products(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS decisions (
  id TEXT PRIMARY KEY,
  user_id TEXT NOT NULL DEFAULT 'demo',
  analysis_id TEXT,
  product_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decision TEXT,
  notes TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_decisions_user_created ON decisions(user_id, created_at DESC);
