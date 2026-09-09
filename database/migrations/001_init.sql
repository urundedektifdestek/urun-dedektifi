-- Ürün Dedektifi Milestone 1 Database Schema

create table if not exists analyses (
  id text primary key,
  created_at timestamptz default now(),
  user_id text,
  query text,
  input jsonb,
  ai_council jsonb,
  evidence_gate jsonb,
  products jsonb
);

create table if not exists decisions (
  id text primary key,
  created_at timestamptz default now(),
  user_id text,
  product_id text,
  decision text,
  note text,
  payload jsonb
);

create table if not exists products (
  id text primary key,
  created_at timestamptz default now(),
  first_seen_at timestamptz default now(),
  last_seen_at timestamptz default now(),
  title text,
  source text,
  source_url text,
  exact_sales_count integer,
  visible_sales_signal text,
  confidence numeric,
  payload jsonb
);

create table if not exists source_observations (
  id text primary key,
  created_at timestamptz default now(),
  product_id text,
  source text,
  method text,
  url text,
  confidence numeric,
  payload jsonb
);

create table if not exists notifications (
  id text primary key,
  created_at timestamptz default now(),
  user_id text,
  level text,
  title text,
  body text,
  dedupe_key text,
  delivered_at timestamptz,
  payload jsonb
);
