import http from "node:http";
import { Pool } from "pg";

const HOST = "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

const CONFIG = {
  app: process.env.PUBLIC_API_NAME || "Ürün Dedektifi API",
  version: "4.3.0-m4.3-module-infra-product-hunter",
  openaiKey: process.env.OPENAI_API_KEY || "",
  openaiModel: process.env.OPENAI_MODEL || "gpt-5-mini",
  apiToken: process.env.API_TOKEN || "",
  databaseUrl: process.env.DATABASE_URL || "",
  linkFetchEnabled: (process.env.LINK_FETCH_ENABLED || "true").toLowerCase() !== "false",
  serpapiKey: process.env.SERPAPI_KEY || "",
  apifyToken: process.env.APIFY_TOKEN || "",
  apifyActorId: process.env.APIFY_ACTOR_ID || "apify~web-scraper",
  apifyTimeoutSecs: Number(process.env.APIFY_TIMEOUT_SECS || 120),
  radarSlowFallbackEnabled: (process.env.RADAR_SLOW_FALLBACK_ENABLED || "false").toLowerCase() === "true",
  defaultUserId: process.env.DEFAULT_USER_ID || "demo"
};

const memory = { analyses: [], saved: [], decisions: [], created_at: new Date().toISOString() };

function now(){ return new Date().toISOString(); }
function id(prefix){ return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2,8)}`; }
function safeText(v){ return v === null || v === undefined ? "" : String(v).trim(); }
function safeInt(v, fallback=0){ const n = Number(v); return Number.isFinite(n) ? Math.round(n) : fallback; }
function uniq(arr){ return [...new Set((arr || []).map(safeText).filter(Boolean))]; }
function clamp(n,min,max,fallback){ const x = Number(n); return Number.isFinite(x) ? Math.max(min, Math.min(max, x)) : fallback; }

let pool = null;
let dbReady = false;
let dbError = null;

if (CONFIG.databaseUrl) {
  pool = new Pool({
    connectionString: CONFIG.databaseUrl,
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 15000,
    ssl: CONFIG.databaseUrl.includes("localhost") || CONFIG.databaseUrl.includes("railway.internal")
      ? false
      : { rejectUnauthorized: false }
  });
}

async function dbQuery(sql, params=[]){
  if (!pool) throw new Error("DATABASE_URL yok");
  return await pool.query(sql, params);
}

async function initDb(){
  if (!pool) return false;
  const sql = `
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

    INSERT INTO source_registry(source,status,health,config) VALUES
      ('Trendyol','configured','ready','{"mode":"public_search_and_product_page"}'::jsonb),
      ('Shopify','adapter_ready','needs_targets','{"requires":"shopify_store_urls_or_search_provider"}'::jsonb),
      ('Meta Ads','adapter_ready','needs_provider','{"requires":"Meta Ad Library/API or browser worker"}'::jsonb),
      ('Alibaba','adapter_ready','needs_provider','{"requires":"supplier search provider/API/manual quote"}'::jsonb),
      ('Yerli Üretim','adapter_ready','needs_provider','{"requires":"supplier directory or manual offers"}'::jsonb)
    ON CONFLICT (source) DO NOTHING;

  `;
  await dbQuery(sql);
  dbReady = true;
  dbError = null;
  return true;
}

const dbInitPromise = initDb().catch(e => {
  dbReady = false;
  dbError = e?.message || "db_init_error";
  console.error("db_init_error", e);
});

function send(res, status, data){
  res.writeHead(status, {
    "Content-Type":"application/json; charset=utf-8",
    "Access-Control-Allow-Origin":"*",
    "Access-Control-Allow-Methods":"GET,POST,OPTIONS",
    "Access-Control-Allow-Headers":"Content-Type,Authorization,X-API-Token",
    "Cache-Control":"no-store"
  });
  res.end(JSON.stringify(data, null, 2));
}

async function readBody(req){
  return await new Promise((resolve) => {
    let raw = "";
    req.on("data", chunk => { raw += chunk; if (raw.length > 2000000) req.destroy(); });
    req.on("end", () => {
      if (!raw) return resolve({});
      try { resolve(JSON.parse(raw)); } catch { resolve({ raw_text: raw }); }
    });
    req.on("error", () => resolve({}));
  });
}

function getUserId(req, url, body={}){
  return safeText(body.user_id || body.userId || url?.searchParams?.get("user_id") || req.headers["x-user-id"] || CONFIG.defaultUserId) || "demo";
}

function checkAuth(req){
  if (!CONFIG.apiToken) return null;
  const auth = safeText(req.headers.authorization);
  const token = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : safeText(req.headers["x-api-token"]);
  return token === CONFIG.apiToken ? null : { ok:false, error:"unauthorized" };
}

/* -------------------- Product Link Evidence Adapter -------------------- */

function detectSource(productUrl){
  const u = safeText(productUrl).toLowerCase();
  if (u.includes("trendyol.")) return "Trendyol";
  if (u.includes("shopify") || u.includes("myshopify.com")) return "Shopify";
  if (u.includes("etsy.")) return "Etsy";
  if (u.includes("amazon.")) return "Amazon";
  if (u.includes("alibaba.")) return "Alibaba";
  return productUrl ? "Public Link" : "AI Oda";
}

function isHttpUrl(v){
  try { const u = new URL(v); return u.protocol === "http:" || u.protocol === "https:"; }
  catch { return false; }
}

function decodeEntities(s){
  return safeText(s)
    .replace(/&quot;/g,'"').replace(/&#34;/g,'"')
    .replace(/&#x27;/g,"'").replace(/&#39;/g,"'")
    .replace(/&amp;/g,"&").replace(/&lt;/g,"<").replace(/&gt;/g,">")
    .replace(/\s+/g," ").trim();
}

function stripTags(s){
  return decodeEntities(safeText(s)
    .replace(/<script[\s\S]*?<\/script>/gi," ")
    .replace(/<style[\s\S]*?<\/style>/gi," ")
    .replace(/<[^>]+>/g," "));
}

function flattenClean(v, max=900){
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return decodeEntities(v).replace(/\s+/g," ").trim().slice(0,max);
  if (typeof v === "number" || typeof v === "boolean") return String(v);
  if (Array.isArray(v)) return v.map(x => flattenClean(x, 220)).filter(Boolean).slice(0,6).join(" • ").slice(0,max);
  if (typeof v === "object") {
    return Object.entries(v).map(([k,val]) => {
      const f = flattenClean(val, 260);
      return f ? `${k}: ${f}` : "";
    }).filter(Boolean).slice(0,8).join(" | ").slice(0,max);
  }
  return String(v).trim().slice(0,max);
}

function meta(html, key){
  const patterns = [
    new RegExp(`<meta[^>]+property=["']${key}["'][^>]+content=["']([^"']+)["'][^>]*>`, "i"),
    new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+property=["']${key}["'][^>]*>`, "i"),
    new RegExp(`<meta[^>]+name=["']${key}["'][^>]+content=["']([^"']+)["'][^>]*>`, "i"),
    new RegExp(`<meta[^>]+content=["']([^"']+)["'][^>]+name=["']${key}["'][^>]*>`, "i")
  ];
  for (const p of patterns) {
    const m = html.match(p);
    if (m?.[1]) return decodeEntities(m[1]);
  }
  return "";
}

function titleTag(html){
  const m = html.match(/<title[^>]*>([\s\S]*?)<\/title>/i);
  return m?.[1] ? decodeEntities(stripTags(m[1])) : "";
}

function parseJsonLd(html){
  const out = [];
  const re = /<script[^>]+type=["']application\/ld\+json["'][^>]*>([\s\S]*?)<\/script>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const raw = decodeEntities(m[1]).trim();
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) out.push(...parsed);
      else if (parsed?.["@graph"] && Array.isArray(parsed["@graph"])) out.push(...parsed["@graph"]);
      else out.push(parsed);
    } catch {}
  }
  return out;
}

function pickProductJsonLd(items){
  for (const it of items || []) {
    const type = it?.["@type"];
    const types = Array.isArray(type) ? type.map(String) : [String(type || "")];
    if (types.some(t => t.toLowerCase().includes("product"))) return it;
  }
  return null;
}

function imageListFromJsonLd(product){
  const img = product?.image;
  if (!img) return [];
  if (Array.isArray(img)) return img.map(x => typeof x === "string" ? x : x?.url).filter(Boolean);
  if (typeof img === "string") return [img];
  if (img?.url) return [img.url];
  return [];
}

function normalizeImageUrl(url){
  let u = safeText(url)
    .replace(/\\u002F/g, "/")
    .replace(/\\/g, "")
    .replace(/&amp;/g, "&")
    .replace(/^"+|"+$/g, "")
    .trim();
  return u;
}

function isLikelyProductImage(url, source){
  const u = safeText(url).toLowerCase();
  if (!u) return false;

  const bad = [
    "apple-icon", "splash", "favicon", "logo", "sprite", "placeholder",
    "default-image", "app-icon", "google-play", "appstore",
    "/sfweb/images/", "/web/images/", "social", "footer", "header"
  ];
  if (bad.some(x => u.includes(x))) return false;
  if (!/\.(jpg|jpeg|png|webp)(\?|$)/i.test(u)) return false;

  if (source === "Trendyol") {
    if (!u.includes("cdn.dsmcdn.com")) return false;
    if (!u.includes("/prod/")) return false;
    if (!/(org_zoom|zoom|product|prod|ty\d+)/i.test(url)) return false;
  }

  return true;
}

function scoreImageUrl(url, source){
  const u = safeText(url).toLowerCase();
  let score = 0;
  if (u.includes("cdn.dsmcdn.com")) score += 10;
  if (u.includes("/prod/")) score += 20;
  if (u.includes("org_zoom")) score += 30;
  if (u.includes("zoom")) score += 10;
  if (u.includes("/1_") || u.includes("/1_org") || u.includes("-1-")) score += 3;
  if (u.includes("apple-icon") || u.includes("splash") || u.includes("/sfweb/images/")) score -= 100;
  return score;
}

function findImageUrls(html, source){
  const urls = [];
  const genericRe = /https?:\/\/[^"'<>\\\s]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'<>\\\s]*)?/gi;
  let m;
  while ((m = genericRe.exec(html)) !== null) urls.push(normalizeImageUrl(m[0]));
  const dsmRe = /https?:\/\/[^"'<>\\\s]*cdn\.dsmcdn\.com[^"'<>\\\s]*/gi;
  while ((m = dsmRe.exec(html)) !== null) {
    const candidate = normalizeImageUrl(m[0]);
    if (/\.(jpg|jpeg|png|webp)/i.test(candidate)) urls.push(candidate);
  }
  return uniq(urls)
    .map(normalizeImageUrl)
    .filter(u => isLikelyProductImage(u, source))
    .sort((a,b) => scoreImageUrl(b, source) - scoreImageUrl(a, source))
    .slice(0, 16);
}

function extractPrice(html, product){
  const offer = Array.isArray(product?.offers) ? product.offers[0] : product?.offers;
  const p = offer?.price || offer?.lowPrice || offer?.highPrice || "";
  const currency = offer?.priceCurrency || "TRY";
  if (p) return { visible_price: String(p), currency, evidence: "json_ld_offer" };

  const candidates = [
    /"sellingPrice"\s*:\s*"?([0-9]+(?:[.,][0-9]+)?)"?/i,
    /"discountedPrice"\s*:\s*"?([0-9]+(?:[.,][0-9]+)?)"?/i,
    /"price"\s*:\s*"?([0-9]+(?:[.,][0-9]+)?)"?/i,
    /([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*TL/i
  ];
  for (const re of candidates) {
    const m = html.match(re);
    if (m?.[1]) return { visible_price: m[1], currency:"TRY", evidence:"html_regex" };
  }
  return null;
}

function extractRating(product, html=""){
  const rating = product?.aggregateRating;
  if (rating) {
    return {
      rating_value: rating.ratingValue ? String(rating.ratingValue) : null,
      review_count: rating.reviewCount || rating.ratingCount ? Number(rating.reviewCount || rating.ratingCount) : null,
      evidence: "json_ld_aggregateRating"
    };
  }

  const ratingValue = html.match(/"ratingValue"\s*:\s*"?([0-9.]+)"?/i)?.[1] || "";
  const reviewCount = html.match(/"reviewCount"\s*:\s*"?([0-9]+)"?/i)?.[1] || html.match(/"ratingCount"\s*:\s*"?([0-9]+)"?/i)?.[1] || "";
  if (ratingValue || reviewCount) {
    return {
      rating_value: ratingValue || null,
      review_count: reviewCount ? Number(reviewCount) : null,
      evidence: "html_regex_rating"
    };
  }
  return null;
}

function extractBrand(product, html=""){
  const b = product?.brand;
  if (b) {
    if (typeof b === "string") return decodeEntities(b);
    if (b.name) return decodeEntities(b.name);
  }
  const m = html.match(/"brand"\s*:\s*\{[^}]*"name"\s*:\s*"([^"]+)"/i) || html.match(/"brandName"\s*:\s*"([^"]+)"/i);
  return m?.[1] ? decodeEntities(m[1]) : "";
}

function cleanSellerNameM4(s){
  s = decodeEntities(safeText(s));
  if (!s) return "";
  if (s.length > 80) return "";
  const bad = ["{merchant}", "\u003c", "config", "installment", "payment", "landing", "layoutPageType", "PRODUCT_DETAIL", "price.history"];
  if (bad.some(x => s.toLowerCase().includes(x.toLowerCase()))) return "";
  if (/[{}\[\]]/.test(s)) return "";
  return s;
}

function extractSellerHint(html){
  const patterns = [
    /"merchantName"\s*:\s*"([^"]+)"/i,
    /"sellerName"\s*:\s*"([^"]+)"/i,
    /"supplierName"\s*:\s*"([^"]+)"/i,
    /"storeName"\s*:\s*"([^"]+)"/i,
    /Satıcı:\s*([^<\n]+)/i
  ];
  for (const p of patterns) {
    const m = html.match(p);
    if (m?.[1]) return decodeEntities(m[1]);
  }
  return "";
}

function extractVisibleSalesSignal(html){
  const text = stripTags(html).slice(0, 500000);
  const patterns = [
    /(son\s+\d+\s+günde\s+[0-9.+]+\s*(?:adet|ürün)?\s*satıldı)/i,
    /([0-9.+]+\s*(?:adet|ürün)?\s*satıldı)/i,
    /([0-9.+]+\s*kişi\s+satın\s+aldı)/i,
    /([0-9.+]+\s*ürün\s+satıldı)/i
  ];
  for (const p of patterns) {
    const m = text.match(p);
    if (m?.[1]) return {
      visible_sales_signal: decodeEntities(m[1]),
      exact_sales_count: null,
      evidence: "visible_text_regex",
      warning: "Bu exact satış sayısı değildir; görünür satış sinyali olarak saklanır."
    };
  }
  return null;
}

function extractCategoryHints(html){
  const out = [];
  const crumbs = [];
  const crumbRe = /"name"\s*:\s*"([^"]{2,80})"\s*,\s*"item"/gi;
  let m;
  while((m = crumbRe.exec(html)) !== null) crumbs.push(decodeEntities(m[1]));
  if (crumbs.length) out.push(...crumbs);
  const metaSection = meta(html, "product:section") || meta(html, "category");
  if (metaSection) out.push(metaSection);
  return uniq(out).slice(0, 8);
}

async function fetchWithTimeout(url, ms=15000){
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    const r = await fetch(url, {
      signal: ctrl.signal,
      headers: {
        "User-Agent": "Mozilla/5.0 (compatible; UrunDedektifiBot/1.0; +https://urun-dedektifi-production.up.railway.app)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.7,en;q=0.6"
      }
    });
    const body = await r.text();
    return {status:r.status, ok:r.ok, final_url:r.url, html:body};
  } finally {
    clearTimeout(t);
  }
}

async function readProductEvidence(productUrl){
  const url = safeText(productUrl);
  const source = detectSource(url);
  const base = {
    ok:false,
    source,
    url,
    fetched_at:now(),
    title:"",
    description:"",
    brand_name:"",
    seller_name:"",
    categories:[],
    images:[],
    removed_non_product_images:[],
    product_video:null,
    price:null,
    rating:null,
    visible_sales_signal:null,
    exact_sales_count:null,
    reviews:[],
    review_status:"not_available",
    limitations:[],
    evidence_quality:"none",
    evidence_summary: {
      title:false, description:false, images:false, price:false, rating:false, seller:false, visible_sales_signal:false
    }
  };

  if (!url) return {...base, error:"url_empty", limitations:["Ürün linki yok."]};
  if (!isHttpUrl(url)) return {...base, error:"invalid_url", limitations:["Geçerli http/https linki değil."]};
  if (!CONFIG.linkFetchEnabled) return {...base, error:"link_fetch_disabled", limitations:["LINK_FETCH_ENABLED=false."]};

  try {
    const fetched = await fetchWithTimeout(url);
    base.http_status = fetched.status;
    base.final_url = fetched.final_url;

    if (!fetched.ok || !fetched.html) {
      return {
        ...base,
        error:`fetch_failed_${fetched.status}`,
        limitations:[`Sayfa alınamadı. HTTP ${fetched.status}`, "Bazı pazaryerleri bot trafiğini engelleyebilir."]
      };
    }

    const html = fetched.html;
    const jsonLd = parseJsonLd(html);
    const productLd = pickProductJsonLd(jsonLd);

    const ogTitle = meta(html,"og:title");
    const ogDesc = meta(html,"og:description") || meta(html,"description");
    const ogImage = meta(html,"og:image") || meta(html,"twitter:image");

    const title = flattenClean(productLd?.name || ogTitle || titleTag(html), 500);
    const description = flattenClean(productLd?.description || ogDesc, 900);

    const rawImages = uniq([ogImage, ...imageListFromJsonLd(productLd), ...findImageUrls(html, source)]).map(normalizeImageUrl);
    const images = rawImages
      .filter(u => isLikelyProductImage(u, source))
      .sort((a,b) => scoreImageUrl(b, source) - scoreImageUrl(a, source))
      .slice(0, 12);
    const removed_non_product_images = rawImages.filter(u => !isLikelyProductImage(u, source)).slice(0, 20);

    const price = extractPrice(html, productLd);
    const rating = extractRating(productLd, html);
    const visibleSales = extractVisibleSalesSignal(html);
    const seller = cleanSellerNameM4(extractSellerHint(html));
    const brand = extractBrand(productLd, html);
    const categories = extractCategoryHints(html);

    const limitations = [];
    if (!images.length) limitations.push("Ürün fotoğrafı bulunamadı veya sayfa dinamik yüklüyor.");
    if (removed_non_product_images.length) limitations.push(`${removed_non_product_images.length} adet ürün dışı ikon/splash görseli filtrelendi.`);
    if (!rating) limitations.push("Puan/yorum sayısı bulunamadı.");
    if (!seller) limitations.push("Satıcı adı güvenilir şekilde bulunamadı.");
    if (!visibleSales) limitations.push("Görünür satış sinyali bulunamadı.");
    limitations.push("Yorum metinleri bu aşamada çekilmiyor; yorum adapterı sonraki aşamada bağlanacak.");
    limitations.push("Exact satış sayısı üretilmedi.");

    const evidence_summary = {
      title:!!title,
      description:!!description,
      images:images.length > 0,
      price:!!price,
      rating:!!rating,
      seller:!!seller,
      visible_sales_signal:!!visibleSales
    };
    const score = Object.values(evidence_summary).filter(Boolean).length;
    const evidence_quality = score >= 5 ? "high" : score >= 3 ? "medium" : score >= 1 ? "low" : "none";

    return {
      ...base,
      ok:true,
      title,
      description,
      brand_name:brand,
      seller_name:seller,
      categories,
      images,
      removed_non_product_images,
      product_video:null,
      price,
      rating,
      visible_sales_signal:visibleSales,
      exact_sales_count:null,
      review_status:"pending_review_adapter",
      limitations,
      evidence_quality,
      evidence_summary
    };
  } catch(e) {
    return {
      ...base,
      error:e?.name === "AbortError" ? "fetch_timeout" : "fetch_exception",
      message:e?.message || "Sayfa okunamadı",
      limitations:["Sayfa okunamadı veya kaynak engelledi.", "Bu aşama güvenli fallback ile devam eder."]
    };
  }
}

/* -------------------- AI Council -------------------- */

function outputText(data){
  if (typeof data.output_text === "string" && data.output_text.trim()) return data.output_text.trim();
  const chunks = [];
  for (const item of data.output || []) {
    for (const c of item.content || []) {
      if (typeof c.text === "string") chunks.push(c.text);
      if (c.type === "output_text" && typeof c.text === "string") chunks.push(c.text);
    }
  }
  return chunks.join("\n").trim();
}

function parseJsonLoose(text){
  const clean = safeText(text).replace(/^```json\s*/i,"").replace(/^```\s*/i,"").replace(/```$/i,"").trim();
  try { return JSON.parse(clean); } catch {}
  const a = clean.indexOf("{"), b = clean.lastIndexOf("}");
  if (a >= 0 && b > a) { try { return JSON.parse(clean.slice(a,b+1)); } catch {} }
  return null;
}

function extractString(src,key){
  const re = new RegExp(`"${key}"\\s*:\\s*"((?:\\\\.|[^"\\\\])*)"`, "s");
  const m = safeText(src).match(re);
  if(!m) return "";
  try { return JSON.parse(`"${m[1]}"`); } catch { return m[1]; }
}

function extractNumber(src,key){
  const m = safeText(src).match(new RegExp(`"${key}"\\s*:\\s*(\\d+)`));
  return m ? Number(m[1]) : null;
}

function cleanItem(x){
  const s = flattenClean(x, 260).replace(/^[\s,:;{}\[\]"]+|[\s,:;{}\[\]"]+$/g, "").trim();
  if (!s || s.length < 6) return "";
  if (/^[,:;{}\[\]" ]+$/.test(s)) return "";
  if (s.includes("\n{") || s === ":" || s === ",") return "";
  return s.slice(0,260);
}

function cleanArray(value, fallback=[]){
  let arr = Array.isArray(value) ? value : value ? [value] : [];
  const out = [];
  for (const item of arr) {
    const c = cleanItem(item);
    if (c && !out.includes(c)) out.push(c);
  }
  for (const f of fallback) {
    const c = cleanItem(f);
    if (c && !out.includes(c)) out.push(c);
  }
  return out.slice(0,5);
}

function extractArray(src,key){
  const re = new RegExp(`"${key}"\\s*:\\s*\\[([\\s\\S]*?)\\]`, "s");
  const m = safeText(src).match(re);
  if(!m) return [];
  try { return cleanArray(JSON.parse(`[${m[1]}]`)); } catch {}
  const vals=[]; const itemRe=/"((?:\\.|[^"\\])*)"/g; let im;
  while((im=itemRe.exec(m[1]))!==null){ try{vals.push(JSON.parse(`"${im[1]}"`));}catch{vals.push(im[1]);} }
  return cleanArray(vals);
}

function decisionFromScore(score){ return score>=82?"GOLD":score>=70?"GÜÇLÜ ADAY":score>=55?"İNCELE":score>=40?"RİSKLİ":"PASS"; }

const fallbackCommon = ["Kanıt olmadan winner denmez", "Tedarik maliyeti şart", "Satıcı yoğunluğu ölçülmeli"];
const fallbackMissing = ["Canlı Trendyol/Shopify/Alibaba kanıtı", "Ürün fotoğrafı/video", "Yorum kanıtı", "GTIP/vergi/regülasyon"];

function normalizeCouncil(raw, txt=""){
  const c = raw && typeof raw === "object" ? raw : {};
  const scoreNum = Number(c.score ?? extractNumber(txt,"score"));
  const score = Number.isFinite(scoreNum) ? Math.max(0,Math.min(100,Math.round(scoreNum))) : 50;
  const allowed = ["GOLD","GÜÇLÜ ADAY","İNCELE","RİSKLİ","PASS"];
  const ed = extractString(txt,"decision");
  const decision = allowed.includes(c.decision) ? c.decision : allowed.includes(ed) ? ed : decisionFromScore(score);

  const result = {
    mode:"openai_war_room",
    real_ai:true,
    summary:flattenClean(c.summary, 700) || extractString(txt,"summary") || "Derin AI tartışması üretildi.",
    score,
    decision,
    gpt:flattenClean(c.gpt, 900) || extractString(txt,"gpt") || "Ticari fırsat ve hedef kitle kanıtlarla tartışılmalı.",
    gemini:flattenClean(c.gemini, 900) || extractString(txt,"gemini") || "Trend sinyalleri satış sayısı değildir; pazar sinyalleri doğrulanmalı.",
    claude:flattenClean(c.claude, 900) || extractString(txt,"claude") || "Risk, regülasyon ve marka/IP kanıtı olmadan AL kilitli kalır.",
    deepseek:flattenClean(c.deepseek, 900) || extractString(txt,"deepseek") || "Alibaba/tedarik maliyeti için canlı kaynak adapterı gerekir.",
    common_points:cleanArray(c.common_points, extractArray(txt,"common_points").concat(fallbackCommon)),
    objections:cleanArray(c.objections, extractArray(txt,"objections")),
    missing_evidence:cleanArray(c.missing_evidence, extractArray(txt,"missing_evidence").concat(fallbackMissing)),
    next_actions:cleanArray(c.next_actions, extractArray(txt,"next_actions")),
    judge:flattenClean(c.judge, 600) || extractString(txt,"judge") || "Kritik kanıtlar tamamlanmadan AL kararı kilitli.",
    alibaba_research:flattenClean(c.alibaba_research, 700) || extractString(txt,"alibaba_research") || "Canlı Alibaba araştırması sonraki adapter ile yapılacak.",
    product_strengths:cleanArray(c.product_strengths, extractArray(txt,"product_strengths")),
    review_insights:cleanArray(c.review_insights, extractArray(txt,"review_insights")),
    visual_insights:cleanArray(c.visual_insights, extractArray(txt,"visual_insights"))
  };

  if (result.product_strengths.length === 0) result.product_strengths = result.common_points.slice(0,3);
  if (result.review_insights.length === 0) result.review_insights = ["Canlı yorum adapterı bağlanınca olumlu/olumsuz yorumlar burada sınıflandırılacak."];
  if (result.visual_insights.length === 0) result.visual_insights = ["Ürün fotoğrafı/video kanıtı henüz yok; adapter bağlanınca burada gösterilecek."];
  if (result.next_actions.length === 0) result.next_actions = ["Yakın rakipleri listele", "Tedarikçi teklifleri topla", "GTIP/vergi/regülasyon kontrolü yap"];

  return result;
}

function localCouncil(productEvidence=null){
  const hasEvidence = !!productEvidence?.ok;
  return {
    mode:"local_fallback",
    real_ai:false,
    score: hasEvidence ? 58 : 55,
    decision:"İNCELE",
    summary: hasEvidence ? `Link okundu: ${productEvidence.title || productEvidence.source}. AI yoksa yerel ön analiz.` : "Yerel güvenli ön analiz. Canlı kaynaklar bağlanınca derin veriyle zenginleşir.",
    gpt:"Niş fırsat olabilir; hedef kitle ve kullanım senaryosu netleştirilmeli.",
    gemini:"Trend olumlu olabilir; kreatif içerik ve ürün hikayesi avantaj sağlar.",
    claude:"Regülasyon, kalite, IP ve iade riski kontrol edilmeden AL kilitli.",
    deepseek:"Alibaba canlı maliyet araştırması için adapter gerekir; MOQ, birim fiyat, navlun ve paket hacmi toplanmalı.",
    common_points:fallbackCommon,
    objections:["Canlı veri eksik","Regülasyon belirsiz"],
    missing_evidence: hasEvidence ? ["Yorum metinleri", "Satıcı yoğunluğu", "Canlı Alibaba maliyeti", "GTIP/vergi/regülasyon"] : fallbackMissing,
    next_actions:["Yorum adapterı bağla","Rakip/satıcı yoğunluğunu ölç","Tedarik maliyeti topla"],
    judge:"İNCELE; AL kararı Evidence Gate ile kilitli.",
    alibaba_research:"Canlı Alibaba araması sonraki aşamada bağlanacak.",
    product_strengths:hasEvidence ? ["Ürün sayfasından temel kanıt toplandı"] : ["Niş hedef kitle olabilir"],
    review_insights:["Yorum metni çekimi bekliyor"],
    visual_insights:hasEvidence && productEvidence.images?.length ? [`${productEvidence.images.length} gerçek ürün görseli yakalandı.`] : ["Foto/video çekimi bekliyor"]
  };
}

function buildAiPrompt(payload, productEvidence){
  return `
Sen Ürün Dedektifi AI Savaş Odası'sın. Cevabın SADECE geçerli JSON olacak.

Ürün felsefesi: "Çok satanı değil, bizim satabileceğimiz çok satanı bul."

Format kuralları:
- gpt, gemini, claude, deepseek, judge, alibaba_research alanları STRING olacak. Obje veya array yapma.
- common_points, objections, missing_evidence, next_actions, product_strengths, review_insights, visual_insights alanları ARRAY OF STRING olacak.
- real_ai true olacak.
- mode "openai_war_room" olacak.
- decision sadece şunlardan biri olacak: GOLD, GÜÇLÜ ADAY, İNCELE, RİSKLİ, PASS
- Markdown ve kod bloğu yok.

Roller:
- gpt: ticari fırsat, niş, hedef kitle, ürünün güçlü yönleri.
- gemini: trend, pazar, kreatif, sosyal medya, sürdürülebilirlik.
- claude: acımasız risk itirazı, kalite, iade, regülasyon, marka/IP.
- deepseek: Alibaba/tedarik/maliyet bakışı. Canlı Alibaba verisi yoksa "canlı veri yok" de; MOQ, birim fiyat, navlun, paket hacmi, numune, kalite kontrol, yerli üretim alternatifini tartış. Maliyet uydurma.

Kanıt kuralları:
- Ürün fotoğrafı, yorum, video, satıcı sayısı, satış sinyali yoksa açıkça "kanıt yok" de.
- exact_sales_count uydurma.
- Reklam yoğunluğu satış değildir.
- visible_sales_signal varsa exact satış gibi davranma.
- product_evidence içindeki title/price/rating/images alanlarını kanıt olarak kullan; olmayanı uydurma.

JSON şeması:
{
  "mode":"openai_war_room",
  "real_ai":true,
  "summary":"string",
  "score":0,
  "decision":"İNCELE",
  "gpt":"string",
  "gemini":"string",
  "claude":"string",
  "deepseek":"string",
  "common_points":["string"],
  "objections":["string"],
  "missing_evidence":["string"],
  "next_actions":["string"],
  "judge":"string",
  "alibaba_research":"string",
  "product_strengths":["string"],
  "review_insights":["string"],
  "visual_insights":["string"]
}

Kullanıcı profili: ${JSON.stringify(payload.profile || {})}
Kanallar: Trendyol, Shopify, Etsy, Alibaba, Yerli üretim, CrossMarket, Meta/Instagram, Amazon
Mesaj: ${payload.message || ""}
Link: ${payload.productUrl || ""}
Metin/Yorum: ${payload.productText || ""}

product_evidence:
${JSON.stringify(productEvidence || null, null, 2)}
`.trim();
}

async function openAiCouncil(payload, productEvidence=null){
  if(!CONFIG.openaiKey) return localCouncil(productEvidence);

  try{
    const r = await fetch("https://api.openai.com/v1/responses", {
      method:"POST",
      headers:{"Authorization":`Bearer ${CONFIG.openaiKey}`,"Content-Type":"application/json"},
      body:JSON.stringify({
        model:CONFIG.openaiModel,
        input:buildAiPrompt(payload, productEvidence),
        store:false,
        max_output_tokens:4500
      })
    });

    const data = await r.json().catch(()=>({}));
    if(!r.ok) return {...localCouncil(productEvidence),mode:"openai_error_fallback",openai_error:data?.error?.message || `OpenAI HTTP ${r.status}`};

    const txt = outputText(data);
    const parsed = parseJsonLoose(txt);
    const c = normalizeCouncil(parsed || {}, txt);
    c.mode = parsed ? "openai_war_room" : "openai_clean_extracted";
    c.openai_format_cleaned = true;
    c.evidence_used = productEvidence?.ok ? true : false;
    return c;
  }catch(e){
    return {...localCouncil(productEvidence),mode:"openai_error_fallback",openai_error:e?.message || "OpenAI bağlantı hatası"};
  }
}

/* -------------------- Persistence -------------------- */

async function saveAnalysisToDb(result, userId){
  if (!pool || !dbReady) return {saved:false, reason: dbError || "db_not_ready"};
  const first = result.products?.[0] || {};
  const score = safeInt(result.ai_council?.score ?? first?.score?.opportunity_score, null);
  const decision = safeText(result.ai_council?.decision ?? first?.score?.decision);
  await dbQuery(
    `INSERT INTO analyses
      (id,user_id,created_at,message,product_url,product_text,source,score,decision,ai_council,evidence_gate,products,raw)
     VALUES
      ($1,$2,NOW(),$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,$12::jsonb)
     ON CONFLICT (id) DO NOTHING`,
    [
      result.id,
      userId,
      result.input.message,
      result.input.productUrl,
      result.input.productText,
      first.source || detectSource(result.input.productUrl),
      score,
      decision,
      JSON.stringify(result.ai_council || {}),
      JSON.stringify(result.evidence_gate || {}),
      JSON.stringify(result.products || []),
      JSON.stringify(result)
    ]
  );
  return {saved:true};
}

async function analyze(req,res,body,params,urlObj){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;

  const userId = getUserId(req, urlObj, body);
  const message = safeText(body.message || body.question || body.query || params.get("message") || params.get("q"));
  const productText = safeText(body.productText || body.pasted || body.text || params.get("productText") || params.get("text"));
  const productUrl = safeText(body.productUrl || body.url || params.get("url"));
  const profile = body.profile || {
    budget:Number(body.budget || params.get("budget") || 25000),
    minMargin:Number(body.minMargin || params.get("minMargin") || .25),
    maxSellers:Number(body.maxSellers || params.get("maxSellers") || 4),
    minRating:Number(body.minRating || params.get("minRating") || 4.5),
    minMonthlySales:Number(body.minMonthlySales || params.get("minMonthlySales") || 100)
  };

  if(!message && !productText && !productUrl) return send(res,400,{ok:false,error:"message, productText veya productUrl gerekli"});

  let productEvidence = null;
  if (productUrl && isHttpUrl(productUrl)) {
    productEvidence = await readProductEvidence(productUrl);
  }

  const council = await openAiCouncil({message,productText,productUrl,profile}, productEvidence);
  const source = productEvidence?.source || detectSource(productUrl);
  const productTitle = productEvidence?.title || message || productText.slice(0,80) || "Ürün adayı";
  const productDescription = productEvidence?.description || council.summary;

  const result = {
    ok:true,
    id:id("analysis"),
    app:CONFIG.app,
    version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true, radar_find_fix:true, browser_radar_apify:true, apify_link_extract_fix:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true,
      m43_module_infrastructure:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true,
    created_at:now(),
    user_id:userId,
    input:{message,productText,productUrl,profile},
    product_evidence:productEvidence,
    ai_council:council,
    evidence_gate:{
      locked:Array.isArray(council.missing_evidence) && council.missing_evidence.length > 0,
      reason:"Kritik kanıt eksikse nihai AL kararı açılmaz.",
      missing_evidence:council.missing_evidence || []
    },
    product_images:productEvidence?.images || [],
    product_video:productEvidence?.product_video || null,
    product_description:productDescription,
    products:[{
      id:id("product"),
      title:productTitle,
      source,
      source_provider:council.real_ai ? "openai" : "local_fallback",
      url:productUrl,
      brand_name:productEvidence?.brand_name || "",
      seller_name:productEvidence?.seller_name || "",
      categories:productEvidence?.categories || [],
      price:productEvidence?.price || null,
      rating:productEvidence?.rating || null,
      visible_sales_signal:productEvidence?.visible_sales_signal || null,
      exact_sales_count:null,
      images:productEvidence?.images || [],
      confidence:council.real_ai ? .82 : .45,
      score:{
        opportunity_score:council.score,
        decision:council.decision,
        reasons:council.common_points || [],
        risks:council.objections || [],
        missing_evidence:council.missing_evidence || []
      }
    }],
    persistence:{enabled:!!pool, db_ready:dbReady, saved:false},
    policy:{
      exact_sales_count:"Exact satış sayısı yoksa uydurulmaz.",
      visible_sales_signal:"Görünür satış sinyali exact satış sayısı değildir.",
      ads_policy:"Reklam yoğunluğu satış değildir.",
      evidence_gate:"Kritik kanıt eksikse AL kararı kilitli kalır.",
      live_alibaba:"Canlı Alibaba maliyeti ayrı adapter ile bağlanacak."
    }
  };

  memory.analyses.unshift(result);
  memory.analyses = memory.analyses.slice(0,100);

  try {
    const saved = await saveAnalysisToDb(result, userId);
    result.persistence = {enabled:!!pool, db_ready:dbReady, ...saved};
  } catch(e) {
    result.persistence = {enabled:!!pool, db_ready:false, saved:false, error:e?.message || "db_save_error"};
    console.error("analysis_db_save_error", e);
  }

  return send(res,200,result);
}

async function listHistory(req,res,url){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;
  const userId = getUserId(req, url, {});
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 30)));
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, message, product_url, source, score, decision, ai_council, evidence_gate, products, raw->'product_evidence' AS product_evidence
       FROM analyses
       WHERE user_id=$1
       ORDER BY created_at DESC
       LIMIT $2`,
      [userId, limit]
    );
    return send(res,200,{ok:true,source:"postgres",count:r.rows.length,items:r.rows});
  }
  return send(res,200,{ok:true,source:"memory",count:memory.analyses.length,items:memory.analyses.slice(0,limit)});
}

async function saveProduct(req,res,body,url){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;
  const userId = getUserId(req, url, body);

  const product = body.product || body;
  const productId = safeText(product.id) || id("saved_product");
  const title = safeText(product.title) || "Kaydedilen ürün";
  const source = safeText(product.source) || detectSource(product.url || product.product_url);
  const productUrl = safeText(product.url || product.product_url || body.productUrl || body.url);
  const score = safeInt(product.score?.opportunity_score ?? product.score ?? body.score, null);
  const decision = safeText(product.score?.decision ?? product.decision ?? body.decision);
  const analysisId = safeText(body.analysis_id || body.analysisId);

  const item = {
    id: productId,
    user_id:userId,
    analysis_id:analysisId,
    created_at:now(),
    title, source, product_url:productUrl, score, decision,
    product,
    notes:safeText(body.notes)
  };

  if (pool && dbReady) {
    await dbQuery(
      `INSERT INTO saved_products
        (id,user_id,analysis_id,created_at,title,source,product_url,score,decision,product,notes)
       VALUES
        ($1,$2,$3,NOW(),$4,$5,$6,$7,$8,$9::jsonb,$10)
       ON CONFLICT (id) DO UPDATE SET
        created_at=NOW(),
        title=EXCLUDED.title,
        source=EXCLUDED.source,
        product_url=EXCLUDED.product_url,
        score=EXCLUDED.score,
        decision=EXCLUDED.decision,
        product=EXCLUDED.product,
        notes=EXCLUDED.notes`,
      [item.id,item.user_id,item.analysis_id || null,item.title,item.source,item.product_url || null,item.score,item.decision || null,JSON.stringify(item.product),item.notes || null]
    );
    return send(res,200,{ok:true,source:"postgres",saved:item});
  }

  memory.saved.unshift(item);
  memory.saved = memory.saved.slice(0,100);
  return send(res,200,{ok:true,source:"memory",saved:item});
}

async function listSaved(req,res,url){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;
  const userId = getUserId(req, url, {});
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 30)));
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, analysis_id, title, source, product_url, score, decision, product, notes
       FROM saved_products
       WHERE user_id=$1
       ORDER BY created_at DESC
       LIMIT $2`,
      [userId, limit]
    );
    return send(res,200,{ok:true,source:"postgres",count:r.rows.length,items:r.rows});
  }
  return send(res,200,{ok:true,source:"memory",count:memory.saved.length,items:memory.saved.slice(0,limit)});
}

async function saveDecision(req,res,body,url){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;
  const userId = getUserId(req, url, body);

  const item = {
    id:id("decision"),
    user_id:userId,
    analysis_id:safeText(body.analysis_id || body.analysisId),
    product_id:safeText(body.product_id || body.productId),
    decision:safeText(body.decision || body.status || "İNCELE"),
    notes:safeText(body.notes || body.note),
    payload:body
  };

  if (pool && dbReady) {
    await dbQuery(
      `INSERT INTO decisions (id,user_id,analysis_id,product_id,created_at,decision,notes,payload)
       VALUES ($1,$2,$3,$4,NOW(),$5,$6,$7::jsonb)`,
      [item.id,item.user_id,item.analysis_id || null,item.product_id || null,item.decision,item.notes || null,JSON.stringify(item.payload)]
    );
    return send(res,200,{ok:true,source:"postgres",decision:item});
  }

  item.created_at = now();
  memory.decisions.unshift(item);
  return send(res,200,{ok:true,source:"memory",decision:item});
}

async function listDecisions(req,res,url){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;
  const userId = getUserId(req, url, {});
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 30)));
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, analysis_id, product_id, decision, notes, payload
       FROM decisions
       WHERE user_id=$1
       ORDER BY created_at DESC
       LIMIT $2`,
      [userId, limit]
    );
    return send(res,200,{ok:true,source:"postgres",count:r.rows.length,items:r.rows});
  }
  return send(res,200,{ok:true,source:"memory",count:memory.decisions.length,items:memory.decisions.slice(0,limit)});
}

async function searchHistory(req,res,url){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;
  const userId = getUserId(req, url, {});
  const q = safeText(url.searchParams.get("q"));
  const limit = Math.min(50, Math.max(1, Number(url.searchParams.get("limit") || 20)));
  if (!q) return send(res,400,{ok:false,error:"q gerekli"});
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, message, product_url, source, score, decision, products, raw->'product_evidence' AS product_evidence
       FROM analyses
       WHERE user_id=$1 AND (
         message ILIKE $2 OR
         product_url ILIKE $2 OR
         source ILIKE $2 OR
         raw::text ILIKE $2
       )
       ORDER BY created_at DESC
       LIMIT $3`,
      [userId, `%${q}%`, limit]
    );
    return send(res,200,{ok:true,source:"postgres",q,count:r.rows.length,items:r.rows});
  }
  const items = memory.analyses.filter(x => JSON.stringify(x).toLowerCase().includes(q.toLowerCase())).slice(0,limit);
  return send(res,200,{ok:true,source:"memory",q,count:items.length,items});
}


/* -------------------- M4 Automatic Product Discovery / Radar -------------------- */

function featureMatrix(){
  return {
    ok:true,
    app:CONFIG.app,
    version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true,
    features:[
      {name:"Otomatik Ürün Keşfi",status:"live",note:"/radar/run önce fetch dener; engellenirse Apify browser adapter ile ürün adaylarını toplar."},
      {name:"Trendyol Market Intelligence",status:"partial",note:"Başlık, fiyat, puan, yorum sayısı, görsel, marka ve public sayfa kanıtı."},
      {name:"Ürün Snapshot Geçmişi",status:"live",note:"Fiyat/puan/yorum sayısı zaman içinde product_snapshots tablosuna yazılır."},
      {name:"Momentum Motoru",status:"live",note:"Yeni ürün, yorum artışı, fiyat değişimi ve skor hareketi hesaplanır."},
      {name:"Fırsat Skoru",status:"live",note:"Talep sinyali, puan, fiyat, yeni ürün ve momentumdan 0-100 skor üretir."},
      {name:"Fırsat Alarmları",status:"live",note:"Yeni ürün, yüksek skor ve momentum için kalıcı alert üretir."},
      {name:"AI Council",status:CONFIG.openaiKey?"live":"partial",note:"OpenAI bağlıysa ilk adayları tartışır; diğer modeller adapter mimarisinde hazır."},
      {name:"Beni Bekleyenler",status:"live",note:"Kaydedilen ürünler Postgres'te tutulur."},
      {name:"Shopify Intelligence",status:"adapter_ready",note:"Kayıt ve ekran yapısı hazır; canlı mağaza/arama providerı bağlanmalı."},
      {name:"Meta / Instagram Ads",status:"schema_live_provider_ready",note:"/ads/meta endpointi reklam hareketini satış değil momentum sinyali olarak saklar."},
      {name:"Alibaba Sourcing",status:"schema_live_provider_ready",note:"/sourcing/alibaba endpointi, teklif şeması ve manuel teklif kaydı hazır; canlı sağlayıcı bağlanınca veri toplar."},
      {name:"Yerli Üretim",status:"schema_live_provider_ready",note:"/production/local endpointi, üretim sınıflandırma ve manuel atölye teklif kaydı hazır."},
      {name:"Maliyet / ROI",status:"calculation_ready",note:"/cost/roi endpointi girilen maliyet kanıtlarıyla net kâr, marj ve ROI hesaplar; eksikte kilitler."},
      {name:"SaaS / Auth / Plan",status:"plan_schema_ready",note:"/account/plan endpointi, günlük tarama/AI/kayıt limitleri ve plan şeması hazır."},
      {name:"Owner Intelligence",status:"partial_live",note:"/owner/dashboard endpointi ürün, kayıt, karar, alarm, modül ve maliyet sayılarını verir."}
    ]
  };
}

function parseMoney(v){
  if (v === null || v === undefined) return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  let s = String(v).replace("TL","").replace("₺","").replace(/[^0-9,\.]/g,"").trim();
  if (!s) return null;
  if (s.includes(",")) s = s.replace(/\./g,"").replace(",",".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function tFirst(o, keys){
  if (!o || typeof o !== "object") return "";
  for (const k of keys) {
    const v = o[k];
    if (v === null || v === undefined) continue;
    if (typeof v === "string" || typeof v === "number" || typeof v === "boolean") {
      const s = flattenClean(v, 300);
      if (s) return s;
    }
  }
  return "";
}

function tNestedName(o, key){
  const n = o?.[key];
  if (n && typeof n === "object") return tFirst(n, ["name","title"]);
  return "";
}

function normalizeTrendyolUrl(u){
  u = safeText(u).replace(/\\u002F/g,"/").replace(/\\\//g,"/");
  if (!u) return "";
  if (u.startsWith("//")) u = "https:" + u;
  if (u.startsWith("/")) u = "https://www.trendyol.com" + u;
  if (!u.startsWith("http") && u.includes("-p-")) u = "https://www.trendyol.com/" + u;
  const q = u.indexOf("?"); if (q > 0) u = u.slice(0,q);
  return u;
}

function normalizeTrendyolImage(u){
  u = safeText(u).replace(/\\u002F/g,"/").replace(/\\\//g,"/");
  if (!u) return "";
  if (u.startsWith("//")) return "https:" + u;
  if (u.startsWith("http")) return u;
  if (u.startsWith("ty") || u.startsWith("mnresize") || u.startsWith("prod")) return "https://cdn.dsmcdn.com/" + u;
  return u;
}

function extractContentIdFromUrl(url){
  const m = safeText(url).match(/-p-(\d+)/);
  return m ? m[1] : "";
}

function findProductImageObj(o){
  const arr = o?.images || o?.imageUrls || o?.imagesWithOverlay;
  if (Array.isArray(arr) && arr.length) {
    const x = arr[0];
    if (typeof x === "string") return normalizeTrendyolImage(x);
    if (x?.url) return normalizeTrendyolImage(x.url);
  }
  return normalizeTrendyolImage(tFirst(o, ["image","imageUrl","imageUrlTemplate","thumbnail"]));
}

function findProductPriceObj(o){
  const direct = tFirst(o,["price","salePrice","sellingPrice","discountedPrice"]);
  const nDirect = parseMoney(direct); if (nDirect !== null) return nDirect;
  const p = o?.price || o?.priceInfo || o?.pricing;
  if (p && typeof p === "object") {
    const keys = ["discountedPrice","sellingPrice","originalPrice","price","value"];
    for (const k of keys) {
      const v = p[k];
      if (v && typeof v === "object") { const n = parseMoney(tFirst(v,["text","value","price","amount"])); if (n !== null) return n; }
      const n = parseMoney(v); if (n !== null) return n;
    }
  }
  return null;
}

function findProductRatingObj(o){
  const rs = o?.ratingScore || o?.aggregateRating || o?.rating;
  if (rs && typeof rs === "object") {
    const n = Number(rs.averageRating || rs.ratingValue || rs.value || rs.rating || 0);
    return Number.isFinite(n) && n > 0 ? n : null;
  }
  const n = Number(tFirst(o,["ratingValue","averageRating","rating"]));
  return Number.isFinite(n) && n > 0 ? n : null;
}

function findReviewCountObj(o){
  const rs = o?.ratingScore || o?.aggregateRating || o?.socialProof;
  if (rs && typeof rs === "object") {
    const n = Number(rs.totalCount || rs.reviewCount || rs.commentCount || rs.ratingCount || rs.count || -1);
    if (Number.isFinite(n) && n >= 0) return Math.round(n);
  }
  const n = Number(tFirst(o,["reviewCount","commentCount","ratingCount","totalCount"]));
  return Number.isFinite(n) && n >= 0 ? Math.round(n) : null;
}

function collectCandidateObjects(node, out=[]){
  if (!node || out.length > 120) return out;
  if (Array.isArray(node)) { for (const x of node) collectCandidateObjects(x,out); return out; }
  if (typeof node === "object") {
    const url = tFirst(node,["url","productUrl","link"]);
    const title = tFirst(node,["name","title","productName"]);
    if ((title || node.contentId || node.id) && (normalizeTrendyolUrl(url).includes("-p-") || node.contentId || node.productId)) out.push(node);
    for (const v of Object.values(node)) collectCandidateObjects(v,out);
  }
  return out;
}

function parseTrendyolProduct(o, query){
  const url = normalizeTrendyolUrl(tFirst(o,["url","productUrl","link"]));
  const idv = safeText(o?.contentId || o?.productId || o?.id || extractContentIdFromUrl(url));
  return {
    product_id:idv,
    source:"Trendyol",
    product_url:url,
    title:tFirst(o,["name","title","productName"]),
    brand:tNestedName(o,"brand") || tFirst(o,["brandName"]),
    seller:cleanSellerNameM4(tNestedName(o,"merchant") || tFirst(o,["merchantName","sellerName"])),
    image:findProductImageObj(o),
    current_price:findProductPriceObj(o),
    rating_value:findProductRatingObj(o),
    review_count:findReviewCountObj(o),
    query,
    exact_sales_count:null,
    visible_sales_signal:null,
    categories:[],
    raw:o
  };
}

function scoreOpportunity(p, delta={}, params={}){
  let score = 24;
  const rating = Number(p.rating_value || 0);
  const rc = Number(p.review_count || 0);
  const price = Number(p.current_price || 0);
  const budget = Number(params.budget || 0);
  if (rating >= 4.8) score += 18; else if (rating >= 4.5) score += 14; else if (rating >= 4.2) score += 9; else if (rating > 0) score += 3;
  if (rc >= 1000) score += 16; else if (rc >= 300) score += 13; else if (rc >= 100) score += 10; else if (rc >= 30) score += 6; else if (rc >= 1) score += 3;
  if (delta.is_new) score += 12;
  if (delta.review_delta > 0) score += Math.min(18, 6 + Math.floor(delta.review_delta / 10));
  if (delta.price_delta_pct < -5) score += 5;
  if (price > 0 && budget > 0 && price <= budget) score += 7;
  if (p.image) score += 4;
  if (!p.current_price) score -= 6;
  if (!p.rating_value) score -= 5;
  return Math.max(0, Math.min(100, Math.round(score)));
}

function decisionLabel(score){
  return score>=82 ? "GOLD" : score>=70 ? "GÜÇLÜ ADAY" : score>=55 ? "İNCELE" : score>=40 ? "RİSKLİ" : "PASS";
}

async function fetchTrendyolSearch(query, page=1){
  const enc = encodeURIComponent(query);
  const url = `https://public.trendyol.com/discovery-web-searchgw-service/v2/api/infinite-scroll/sr?culture=tr-TR&storefrontId=1&channelId=1&q=${enc}&pi=${page}`;
  const r = await fetchWithTimeout(url, 18000);
  if (!r.ok) throw new Error(`Trendyol search HTTP ${r.status}`);
  try { return JSON.parse(r.html); } catch { return {html:r.html}; }
}


function queryVariants(q){
  const raw = safeText(q);
  const out = [raw];
  const lower = raw.toLowerCase();
  const tr = lower
    .replaceAll("cantasi", "çantası")
    .replaceAll("canta", "çanta")
    .replaceAll("okul cantası", "okul çantası")
    .replaceAll("ahsap", "ahşap")
    .replaceAll("isik", "ışık")
    .replaceAll("urun", "ürün")
    .replaceAll("cocuk", "çocuk")
    .replaceAll("bebek arabasi", "bebek arabası");
  if (tr && tr !== raw) out.push(tr);
  return uniq(out).slice(0, 3);
}

async function fetchTrendyolSearchHtml(query, page=1){
  const enc = encodeURIComponent(query);
  const url = `https://www.trendyol.com/sr?q=${enc}&qt=${enc}&st=${enc}&os=1&pi=${page}`;
  const r = await fetchWithTimeout(url, 20000);
  if (!r.ok) throw new Error(`Trendyol sr HTML HTTP ${r.status}`);
  return r.html || "";
}

function extractTrendyolLinksFromHtml(html){
  const links = [];
  const patterns = [
    /href=["']([^"']*-p-\d+[^"']*)["']/gi,
    /"url"\s*:\s*"([^"]*-p-\d+[^"]*)"/gi,
    /"productUrl"\s*:\s*"([^"]*-p-\d+[^"]*)"/gi,
    /(\/[^"'<>\\\s]+-p-\d+[^"'<>\\\s]*)/gi
  ];
  for (const re of patterns) {
    let m;
    while ((m = re.exec(html)) !== null) {
      const u = normalizeTrendyolUrl(decodeEntities(m[1]));
      if (u && u.includes("trendyol.com") && u.includes("-p-")) links.push(u);
    }
  }
  return uniq(links).slice(0, 80);
}

function productEvidenceToRadarProduct(ev, query){
  const rating = ev?.rating || {};
  const price = ev?.price || {};
  const productId = extractContentIdFromUrl(ev?.final_url || ev?.url) || extractContentIdFromUrl(ev?.url) || id("trendyol");
  return {
    product_id: productId,
    source: ev?.source || "Trendyol",
    product_url: ev?.final_url || ev?.url || "",
    title: ev?.title || "Trendyol ürün adayı",
    brand: ev?.brand_name || "",
    seller: cleanSellerNameM4(ev?.seller_name || ""),
    image: Array.isArray(ev?.images) && ev.images.length ? ev.images[0] : "",
    current_price: parseMoney(price.visible_price || price.amount || price.value || ""),
    rating_value: Number(rating.rating_value || rating.value || 0) || null,
    review_count: Number(rating.review_count || rating.rating_count || 0) || null,
    query,
    exact_sales_count: null,
    visible_sales_signal: ev?.visible_sales_signal || null,
    categories: ev?.categories || [],
    raw: ev,
    evidence_source: "trendyol_product_page_fallback"
  };
}

async function collectTrendyolFromApi(query, page, diagnostics){
  const data = await fetchTrendyolSearch(query, page);
  const candidates = collectCandidateObjects(data, []);
  diagnostics.api_pages.push({query, page, candidates:candidates.length});
  return candidates.map(c => parseTrendyolProduct(c, query));
}

async function collectTrendyolFromHtmlFallback(query, page, limit, diagnostics){
  const html = await fetchTrendyolSearchHtml(query, page);
  const links = extractTrendyolLinksFromHtml(html);
  diagnostics.html_pages.push({query, page, links:links.length});
  const products = [];
  for (const link of links.slice(0, Math.max(limit * 2, limit))) {
    try {
      const ev = await readProductEvidence(link);
      if (ev && ev.ok) products.push(productEvidenceToRadarProduct(ev, query));
      if (products.length >= limit) break;
    } catch(e) {
      diagnostics.product_page_errors.push({url:link, error:e?.message || "product_page_error"});
    }
  }
  return products;
}


function buildTrendyolSearchUrl(query, page=1){
  const enc = encodeURIComponent(query);
  return `https://www.trendyol.com/sr?q=${enc}&qt=${enc}&st=${enc}&os=1&pi=${page}`;
}

function apifyActorPath(){
  // Apify actor IDs in URL path use username~actor-name form.
  return safeText(CONFIG.apifyActorId || "apify~web-scraper").replace("/", "~");
}

function toActorId(v){
  return safeText(v).replace("/", "~");
}

function mapSolidcodeMinRating(value){
  const n = Number(value || 0);
  if (!Number.isFinite(n) || n <= 0) return "any";
  if (n >= 4.5) return "4.5";
  if (n >= 4) return "4";
  if (n >= 3) return "3";
  return "any";
}

function buildDedicatedTrendyolInputs(query, limit, params={}){
  const max = Math.max(1, Math.min(50, Number(limit || 10)));
  const actorLimit = Math.max(10, max); // fatihtahta actor requires input.limit >= 10; backend slices to requested max.
  const maxPrice = Number(params.budget || params.maxPrice || 0);
  const minRatingNumber = Number(params.minRating || 0);
  const searchUrl = buildTrendyolSearchUrl(query, 1);

  const solidcodeFilters = {};
  if (Number.isFinite(maxPrice) && maxPrice > 0) solidcodeFilters.maxPrice = Math.round(maxPrice);
  solidcodeFilters.minRating = mapSolidcodeMinRating(minRatingNumber);

  const maximedupreFilters = {};
  if (Number.isFinite(maxPrice) && maxPrice > 0) maximedupreFilters.maxPrice = Math.round(maxPrice);
  if (Number.isFinite(minRatingNumber) && minRatingNumber > 0) maximedupreFilters.minRating = minRatingNumber;

  // M4.1.9: fatihtahta actor proved fastest/reliable in live tests.
  // We put it first so /radar/run doesn't wait on actors with schema/timeout issues.
  return [
    {
      label:"fatihtahta_schema_fast_first",
      actor: toActorId(process.env.APIFY_TRENDYOL_ACTOR_ID_ALLINONE || "fatihtahta/trendyol-scraper"),
      input:{
        startUrls:[searchUrl],
        limit:actorLimit,
        includeReviews:false,
        maxReviewsPerProduct:0
      },
      timeoutSecs:Number(process.env.APIFY_FAST_TIMEOUT_SECS || 75)
    },
    {
      label:"solidcode_schema_fallback",
      actor: toActorId(process.env.APIFY_TRENDYOL_ACTOR_ID || CONFIG.apifyTrendyolActorId || "solidcode/trendyol-scraper"),
      input:{
        searchQueries:[query],
        startUrls:[],
        maxProductsPerSource:max,
        includeReviews:false,
        maxReviewsPerProduct:0,
        sort:"best_match",
        ...solidcodeFilters
      },
      timeoutSecs:Number(process.env.APIFY_FALLBACK_TIMEOUT_SECS || 45)
    },
    {
      label:"maximedupre_schema_fallback",
      actor: toActorId(process.env.APIFY_TRENDYOL_ACTOR_ID_ALT || "maximedupre/trendyol-scraper"),
      input:{
        target:"search",
        searchQueries:[query],
        storefront:"turkey",
        maxProductsPerSource:max,
        includeReviews:false,
        maxReviewsPerProduct:0,
        sort:"relevance",
        ...maximedupreFilters
      },
      timeoutSecs:Number(process.env.APIFY_FALLBACK_TIMEOUT_SECS || 45)
    }
  ];
}

function parseNumberSmart(v){
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number") return Number.isFinite(v) ? v : null;

  let s = String(v).trim();
  if (!s) return null;

  const upper = s.toUpperCase();
  let multiplier = 1;
  if (upper.includes("K")) multiplier = 1000;
  if (upper.includes("M")) multiplier = 1000000;

  s = s.replace(/[^\d.,+-]/g, "");
  if (!s) return null;

  // Turkish format: 1.003,97 => 1003.97
  if (s.includes(",") && s.includes(".")) {
    s = s.replace(/\./g, "").replace(",", ".");
  } else if (s.includes(",")) {
    s = s.replace(",", ".");
  }
  // If only dot exists, keep it as decimal separator: 4.66 stays 4.66
  const n = Number(s);
  return Number.isFinite(n) ? n * multiplier : null;
}

function firstNum(...vals){
  for (const v of vals) {
    const n = parseNumberSmart(v);
    if (n !== null) return n;
  }
  return null;
}

function firstText(...vals){
  for (const v of vals) {
    const s = safeText(v);
    if (s) return s;
  }
  return "";
}

function normalizeImages(raw){
  const candidates = [
    raw?.images,
    raw?.imageUrls,
    raw?.image_urls,
    raw?.media?.images,
    raw?.media?.imageUrls,
    raw?.pictures,
    raw?.product?.images
  ];
  const out = [];
  for (const c of candidates) {
    if (!c) continue;
    if (Array.isArray(c)) {
      for (const x of c) {
        const u = typeof x === "string" ? x : (x?.url || x?.src || x?.image || x?.imageUrl || "");
        if (u) out.push(u);
      }
    } else if (typeof c === "string") {
      out.push(c);
    } else if (typeof c === "object") {
      const u = c.url || c.src || c.image || c.imageUrl || c.primary_image || c.primaryImage || "";
      if (u) out.push(u);
    }
  }

  const singles = [
    raw?.image,
    raw?.imageUrl,
    raw?.image_url,
    raw?.media?.primary_image,
    raw?.media?.primaryImage,
    raw?.product?.image,
    raw?.product?.imageUrl
  ];
  for (const u of singles) if (safeText(u)) out.unshift(safeText(u));

  return uniq(out.filter(Boolean)).slice(0, 12);
}

function normalizeCategories(raw){
  const out = [];
  if (Array.isArray(raw?.categoryPath)) out.push(...raw.categoryPath.map(x => typeof x === "string" ? x : (x?.name || x?.title || "")).filter(Boolean));
  if (Array.isArray(raw?.categories)) out.push(...raw.categories.map(x => typeof x === "string" ? x : (x?.name || x?.title || "")).filter(Boolean));
  if (raw?.category) out.push(typeof raw.category === "string" ? raw.category : (raw.category.name || raw.category.title || raw.category.categoryName || ""));
  return uniq(out.filter(Boolean));
}

function normalizeVisibleSalesSignal(raw){
  return firstText(
    raw?.visibleSalesSignal,
    raw?.salesSignal,
    raw?.soldText,
    raw?.metrics?.order_count_label,
    raw?.metrics?.orderCountLabel,
    raw?.badges?.ranking?.type === "BEST_SELLER" ? ("Çok satan sırası: " + safeText(raw?.badges?.ranking?.title)) : "",
    raw?.badges?.ranking?.type === "FLASH_SALE" ? "Flaş ürün" : ""
  ) || null;
}

function normalizeDedicatedTrendyolItem(item, query){
  const raw = item || {};
  if (safeText(raw.recordType || raw.type).toLowerCase() === "review") return null;

  const priceObj = raw.priceDetails || raw.price || raw.pricing || raw.prices || {};
  const nestedPrice = raw.pricing?.price || {};
  const priceComponents = raw.pricing?.components || {};
  const ratingObj = raw.aggregateRating || raw.rating || raw.ratings || raw.metrics?.rating || {};
  const sellerObj = raw.seller || raw.merchant || raw.store || {};
  const brandObj = raw.brand || {};
  const ids = raw.identifiers || {};

  const productUrl = firstText(
    raw.productUrl,
    raw.url,
    raw.product_url,
    raw.link,
    raw.href,
    raw.canonicalUrl,
    raw.canonical_url,
    raw.product?.url,
    raw.product?.productUrl
  );

  const productId = firstText(
    raw.productId,
    raw.product_id,
    raw.contentId,
    raw.id,
    raw.sku,
    ids.product_id,
    ids.content_id,
    extractContentIdFromUrl(productUrl)
  );

  const images = normalizeImages(raw);

  const visiblePrice = firstNum(
    raw.price,
    raw.salePrice,
    raw.sellingPrice,
    raw.discountedPrice,
    raw.finalPrice,
    raw.current_price,
    raw.currentPrice,
    priceObj.current_price,
    priceObj.currentPrice,
    nestedPrice.current,
    nestedPrice.discounted_price,
    nestedPrice.original_price,
    priceComponents.single_price?.sale_price_numeric,
    priceComponents.recommended_retail_price?.price_numerized,
    priceComponents.recommended_retail_price?.selling_price_numerized,
    priceComponents.recommended_retail_price?.discounted_promotion_price_numerized,
    priceComponents.app_price?.new_price,
    priceObj.sellingPrice,
    priceObj.discountedPrice,
    priceObj.originalPrice,
    priceObj.price,
    priceObj.value,
    priceObj.amount
  );

  const ratingValue = firstNum(
    raw.rating,
    raw.ratingValue,
    raw.averageRating,
    raw.average_rating,
    raw.metrics?.rating?.average_rating,
    raw.metrics?.rating?.averageRating,
    ratingObj.ratingValue,
    ratingObj.value,
    ratingObj.average,
    ratingObj.average_rating
  );

  const reviewCount = firstNum(
    raw.reviewCount,
    raw.commentCount,
    raw.reviewsCount,
    raw.ratingCount,
    raw.review_count,
    raw.metrics?.rating?.total_count,
    raw.metrics?.rating?.totalCount,
    ratingObj.reviewCount,
    ratingObj.ratingCount,
    ratingObj.count,
    ratingObj.total_count
  );

  const favoriteCount = firstNum(
    raw.favorite,
    raw.favoriteCount,
    raw.favorites,
    raw.wishListCount,
    raw.metrics?.favorite_count_label,
    raw.metrics?.favoriteCountLabel,
    raw.socialProof?.favoriteCount
  );

  const qaCount = firstNum(raw.questionCount, raw.qaCount, raw.qAndACount, raw.answeredQuestionsCount);

  const title = firstText(
    raw.name,
    raw.title,
    raw.productName,
    raw.product_title,
    raw.product?.name,
    raw.product?.title
  );

  const brand = firstText(
    typeof brandObj === "string" ? brandObj : brandObj.name,
    raw.brandName,
    raw.brand_name,
    raw.product?.brandName
  );

  const seller = cleanSellerNameM4(firstText(
    typeof sellerObj === "string" ? sellerObj : sellerObj.name,
    sellerObj.sellerName,
    sellerObj.merchantName,
    raw.sellerName,
    raw.merchantName,
    raw.seller_name,
    raw.seller?.name,
    raw.merchant?.name
  ));
  const merchant_id = firstText(raw.merchantId, raw.sellerId, raw.identifiers?.merchant_id, raw.identifiers?.seller_id);

  if (!productId && !productUrl && !title) return null;

  return {
    product_id: productId || id("trendyol_actor"),
    source:"Trendyol",
    product_url: productUrl,
    title: title || "Trendyol ürün adayı",
    brand,
    seller:seller || (merchant_id ? "Satıcı ID: " + merchant_id : ""),
    merchant_id,
    seller_display:seller || (merchant_id ? "Satıcı ID: " + merchant_id : ""),
    image: images[0] || "",
    images,
    current_price: visiblePrice,
    rating_value: ratingValue,
    review_count: reviewCount,
    favorite_count: favoriteCount,
    qa_count: qaCount,
    query,
    exact_sales_count:null,
    visible_sales_signal: normalizeVisibleSalesSignal(raw),
    categories: normalizeCategories(raw),
    raw,
    evidence_source:"apify_dedicated_trendyol_actor"
  };
}

function extractProductsFromDedicatedItems(items, query){
  const out = [];
  const seen = new Set();

  function walk(x){
    if (!x) return;
    if (Array.isArray(x)) {
      for (const v of x) walk(v);
      return;
    }
    if (typeof x !== "object") return;

    const p = normalizeDedicatedTrendyolItem(x, query);
    if (p) {
      const key = p.product_id || p.product_url || p.title;
      if (!seen.has(key)) {
        seen.add(key);
        out.push(p);
      }
    }

    // Some actors return nested product arrays.
    for (const key of ["products","items","data","results","productList","listings"]) {
      if (Array.isArray(x[key])) walk(x[key]);
    }
  }

  walk(items);
  return out;
}

function backendFilterProductsByParams(products, params={}){
  const maxPrice = Number(params.budget || params.maxPrice || 0);
  const minRating = Number(params.minRating || 0);
  const requireImage = String(params.requireImage || "").toLowerCase() === "true";

  return (products || []).filter(p => {
    if (Number.isFinite(maxPrice) && maxPrice > 0 && Number(p.current_price || 0) > maxPrice) return false;
    // Ratingi hiç yoksa silme; yeni ürünü radardan kaçırmayalım, ama puanı düşük kalsın.
    if (Number.isFinite(minRating) && minRating > 0 && p.rating_value !== null && p.rating_value !== undefined) {
      if (Number(p.rating_value) < minRating) return false;
    }
    if (requireImage && !p.image) return false;
    return true;
  });
}

async function runDedicatedTrendyolActor(query, limit, params, diagnostics){
  if (!CONFIG.apifyToken) return [];

  const attempts = buildDedicatedTrendyolInputs(query, limit, params);
  const allProducts = [];

  for (const attempt of attempts) {
    try {
      const actor = toActorId(attempt.actor);
      const timeoutSecs = Math.max(20, Math.min(90, Number(attempt.timeoutSecs || CONFIG.apifyTimeoutSecs || 45)));
      const url = `https://api.apify.com/v2/acts/${actor}/run-sync-get-dataset-items?token=${encodeURIComponent(CONFIG.apifyToken)}&timeout=${encodeURIComponent(timeoutSecs)}`;
      const controller = new AbortController();
      const kill = setTimeout(() => controller.abort(), (timeoutSecs + 10) * 1000);
      let r;
      try {
        r = await fetch(url, {
          method:"POST",
          headers:{"Content-Type":"application/json"},
          body:JSON.stringify(attempt.input),
          signal:controller.signal
        });
      } finally {
        clearTimeout(kill);
      }
      const text = await r.text();
      let data = null;
      try { data = JSON.parse(text); } catch { data = text; }

      const extractedProducts = r.ok ? extractProductsFromDedicatedItems(data, query) : [];
      const products = backendFilterProductsByParams(extractedProducts, params);
      diagnostics.dedicated_actor_runs = diagnostics.dedicated_actor_runs || [];
      diagnostics.dedicated_actor_runs.push({
        actor,
        label:attempt.label,
        timeout_secs:timeoutSecs,
        http_status:r.status,
        ok:r.ok,
        item_count:Array.isArray(data) ? data.length : null,
        products:products.length,
        extracted_products:typeof extractedProducts !== "undefined" ? extractedProducts.length : 0,
        sample:Array.isArray(data) && data[0] ? JSON.stringify(data[0]).slice(0, 500) : String(text).slice(0, 500)
      });

      if (products.length) {
        allProducts.push(...products);
        break;
      }

      // If user must approve/pay for a community actor, record it and try the next actor.
    } catch(e) {
      diagnostics.dedicated_actor_runs = diagnostics.dedicated_actor_runs || [];
      diagnostics.dedicated_actor_runs.push({
        actor:safeText(attempt.actor),
        label:attempt.label,
        ok:false,
        error:e?.message || "dedicated_actor_error"
      });
    }
  }

  return allProducts.slice(0, Math.max(1, Math.min(50, Number(limit || 10))));
}

function buildApifyWebScraperInput(query, page=1, maxLinks=80){
  const startUrl = buildTrendyolSearchUrl(query, page);

  // M4.1.5:
  // apify/web-scraper pageFunction runs inside the browser page context.
  // M4.1.4 used context.page, so debug showed htmlLength: 0.
  // This version uses document/window directly.
  const pageFunction = String.raw`async function pageFunction(context) {
    const { request } = context;
    const wait = async (ms) => {
      if (context.waitFor) return await context.waitFor(ms);
      return await new Promise(r => setTimeout(r, ms));
    };

    const normalize = (u) => {
      if (!u) return '';
      u = String(u)
        .replace(/\\u002F/g, '/')
        .replace(/\\\//g, '/')
        .replace(/&amp;/g, '&')
        .replace(/%2F/gi, '/')
        .trim();

      u = u.replace(/^["'({[]+/, '');
      u = u.split('"')[0].split("'")[0].split('<')[0].split(' ')[0].split('\\\\')[0];

      if (u.startsWith('//')) u = 'https:' + u;
      if (u.startsWith('/')) u = 'https://www.trendyol.com' + u;
      if (!/^https?:\/\//i.test(u) && u.includes('-p-')) {
        u = 'https://www.trendyol.com/' + u.replace(/^\/+/, '');
      }

      const idx = u.indexOf('https://www.trendyol.com');
      if (idx > 0) u = u.slice(idx);

      const q = u.indexOf('?');
      if (q > 0) u = u.slice(0, q);

      return u;
    };

    const scanTextForLinks = (txt) => {
      const out = [];
      if (!txt) return out;
      txt = String(txt)
        .replace(/\\u002F/g, '/')
        .replace(/\\\//g, '/')
        .replace(/&amp;/g, '&');

      const patterns = [
        /https?:\/\/(?:www\.)?trendyol\.com\/[^"'<>\\s]+-p-\d+[^"'<>\\s]*/gi,
        /\/[a-z0-9çğıöşüÇĞİÖŞÜ\-_%.]+\/[^"'<>\\s]+-p-\d+[^"'<>\\s]*/gi,
        /(?:href|url|productUrl|product_url)["'\s:]+([^"'<>\s]+-p-\d+[^"'<>\s]*)/gi
      ];

      for (const re of patterns) {
        let m;
        while ((m = re.exec(txt)) !== null) out.push(normalize(m[1] || m[0]));
      }
      return out;
    };

    const collect = () => {
      const links = [];

      try {
        document.querySelectorAll('a[href]').forEach(a => {
          const h = a.href || a.getAttribute('href') || '';
          if (h.includes('-p-')) links.push(h);
        });
      } catch (e) {}

      try {
        document.querySelectorAll('[href],[data-url],[data-product-url]').forEach(el => {
          ['href','data-url','data-product-url','data-href','to'].forEach(attr => {
            const v = el.getAttribute && el.getAttribute(attr);
            if (v && String(v).includes('-p-')) links.push(v);
          });
        });
      } catch (e) {}

      try {
        const html = document.documentElement ? document.documentElement.outerHTML : '';
        links.push(...scanTextForLinks(html));
      } catch (e) {}

      try {
        document.querySelectorAll('script').forEach(s => {
          const t = s.textContent || '';
          if (t.includes('-p-') || t.includes('productUrl') || t.includes('products')) {
            links.push(...scanTextForLinks(t));
          }
        });
      } catch (e) {}

      try {
        const text = document.body ? document.body.innerText : '';
        links.push(...scanTextForLinks(text));
      } catch (e) {}

      return links;
    };

    const debug = {
      title: '',
      finalUrl: '',
      htmlLength: 0,
      bodyTextSample: '',
      readyState: '',
      anchorCount: 0,
      productAnchorCount: 0,
      scriptCount: 0
    };

    let links = [];

    try {
      await wait(3500);

      for (let i = 0; i < 8; i++) {
        try { window.scrollBy(0, Math.max(750, window.innerHeight || 900)); } catch (e) {}
        await wait(700);
        links.push(...collect());
      }

      await wait(1200);
      links.push(...collect());

      debug.title = document.title || '';
      debug.finalUrl = location.href || request.url;
      debug.readyState = document.readyState || '';
      debug.anchorCount = document.querySelectorAll('a[href]').length;
      debug.productAnchorCount = Array.from(document.querySelectorAll('a[href]')).filter(a => String(a.href || a.getAttribute('href') || '').includes('-p-')).length;
      debug.scriptCount = document.querySelectorAll('script').length;
      const html = document.documentElement ? document.documentElement.outerHTML : '';
      debug.htmlLength = html.length;
      debug.bodyTextSample = document.body ? (document.body.innerText || '').slice(0, 350) : '';

    } catch (e) {
      return {
        ok:false,
        source:'apify_web_scraper_dom',
        url: request.url,
        error:String(e && e.message || e),
        link_count:0,
        links:[],
        product_urls:[],
        debug
      };
    }

    links = Array.from(new Set(
      links
        .map(normalize)
        .filter(u => u.includes('trendyol.com') && u.includes('-p-') && /-p-\d+/.test(u))
    )).slice(0, ${maxLinks});

    return {
      ok:true,
      source:'apify_web_scraper_dom',
      url: request.url,
      final_url: debug.finalUrl || request.url,
      query: ${JSON.stringify(query)},
      page:${page},
      link_count: links.length,
      links,
      product_urls: links,
      debug
    };
  }`;

  return {
    startUrls: [{ url: startUrl }],
    maxRequestsPerCrawl: 1,
    maxConcurrency: 1,
    maxRequestRetries: 1,
    requestHandlerTimeoutSecs: 120,
    navigationTimeoutSecs: 75,
    pageLoadTimeoutSecs: 75,
    pageFunction,
    proxyConfiguration: { useApifyProxy: true },
    browserLog: false,
    debugLog: false
  };
}
async function runApifyActorSync(input, diagnostics){
  if (!CONFIG.apifyToken) throw new Error("APIFY_TOKEN yok");
  const actor = apifyActorPath();
  const timeout = Math.max(30, Math.min(300, Number(CONFIG.apifyTimeoutSecs || 120)));
  const url = `https://api.apify.com/v2/acts/${actor}/run-sync-get-dataset-items?token=${encodeURIComponent(CONFIG.apifyToken)}&timeout=${timeout}&memory=1024`;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), (timeout + 20) * 1000);
  try {
    const r = await fetch(url, {
      method:"POST",
      signal: ctrl.signal,
      headers:{"Content-Type":"application/json", "Accept":"application/json"},
      body: JSON.stringify(input)
    });
    const text = await r.text();
    let data;
    try { data = JSON.parse(text); } catch { data = text; }
    diagnostics.apify_runs.push({actor, http_status:r.status, ok:r.ok, item_count:Array.isArray(data)?data.length:null});
    if (!r.ok) throw new Error(`Apify HTTP ${r.status}: ${typeof data === "string" ? data.slice(0,300) : JSON.stringify(data).slice(0,300)}`);
    return Array.isArray(data) ? data : [];
  } finally {
    clearTimeout(t);
  }
}

function extractLinksFromApifyItems(items){
  const links = [];
  const walk = (x) => {
    if (!x) return;
    if (typeof x === "string") {
      if (x.includes("-p-")) links.push(normalizeTrendyolUrl(x));
      return;
    }
    if (Array.isArray(x)) { for (const it of x) walk(it); return; }
    if (typeof x === "object") {
      if (x.url && String(x.url).includes("-p-")) links.push(normalizeTrendyolUrl(x.url));
      if (x.product_url && String(x.product_url).includes("-p-")) links.push(normalizeTrendyolUrl(x.product_url));
      if (x.productUrl && String(x.productUrl).includes("-p-")) links.push(normalizeTrendyolUrl(x.productUrl));
      if (Array.isArray(x.links)) for (const l of x.links) walk(l);
      if (Array.isArray(x.items)) for (const l of x.items) walk(l);
    }
  };
  for (const item of items || []) walk(item);
  return uniq(links).filter(u => u.includes("trendyol.com") && u.includes("-p-")).slice(0, 120);
}

async function collectTrendyolFromApify(query, page, limit, diagnostics){
  if (!CONFIG.apifyToken) {
    diagnostics.apify_required = true;
    diagnostics.apify_message = "Railway IP Trendyol aramadan 403 aldığı için APIFY_TOKEN gerekli.";
    return [];
  }
  const input = buildApifyWebScraperInput(query, page, Math.max(limit * 3, 30));
  const datasetItems = await runApifyActorSync(input, diagnostics);
  const links = extractLinksFromApifyItems(datasetItems);
  diagnostics.apify_pages.push({
    query,
    page,
    links:links.length,
    item_count:Array.isArray(datasetItems)?datasetItems.length:0,
    first_item_debug:Array.isArray(datasetItems) && datasetItems[0] ? datasetItems[0].debug || null : null
  });
  const products = [];
  for (const link of links.slice(0, Math.max(limit * 2, limit))) {
    try {
      const ev = await readProductEvidence(link);
      if (ev && ev.ok) products.push(productEvidenceToRadarProduct(ev, query));
      if (products.length >= limit) break;
    } catch(e) {
      diagnostics.product_page_errors.push({url:link, error:e?.message || "product_page_error"});
    }
  }
  return products;
}




async function previousSnapshot(productId, userId){
  if (!pool || !dbReady || !productId) return null;
  const r = await dbQuery(`SELECT price, rating_value, review_count FROM product_snapshots WHERE product_id=$1 AND user_id=$2 ORDER BY created_at DESC LIMIT 1`, [productId,userId]);
  return r.rows[0] || null;
}

function calcDelta(prev, p){
  const out = {is_new:!prev, price_delta:null, price_delta_pct:null, rating_delta:null, review_delta:null};
  if (!prev) return out;
  const price = Number(p.current_price || 0), old = Number(prev.price || 0);
  if (price>0 && old>0) { out.price_delta = +(price-old).toFixed(2); out.price_delta_pct = +(((price-old)/old)*100).toFixed(2); }
  const rating = Number(p.rating_value || 0), oldr = Number(prev.rating_value || 0);
  if (rating>0 && oldr>0) out.rating_delta = +(rating-oldr).toFixed(2);
  const rc = Number(p.review_count ?? -1), oldc = Number(prev.review_count ?? -1);
  if (rc>=0 && oldc>=0) out.review_delta = rc-oldc;
  return out;
}

function momentumLabel(delta){
  if (delta.is_new) return "Yeni ürün";
  if ((delta.review_delta || 0) >= 50) return "Hızlı yorum artışı";
  if ((delta.review_delta || 0) > 0) return `Yorum +${delta.review_delta}`;
  if ((delta.price_delta_pct || 0) <= -10) return "Fiyat düştü";
  if ((delta.price_delta_pct || 0) >= 10) return "Fiyat arttı";
  return "İzleniyor";
}

async function storeDiscoveryProduct(p, userId, runId, params){
  const productId = p.product_id || extractContentIdFromUrl(p.product_url) || id("trendyol");
  p.product_id = productId;
  const prev = await previousSnapshot(productId, userId);
  const delta = calcDelta(prev, p);
  const score = scoreOpportunity(p, delta, params);
  const decision = decisionLabel(score);
  const momentum = momentumLabel(delta);
  const evidence = {
    source:p.source,
    query:p.query,
    method:p.evidence_source || "trendyol_discovery",
    confidence:p.title && p.product_url ? 0.78 : 0.48,
    delta,
    run_id:runId,
    exact_sales_count:null,
    warning:"Exact satış sayısı uydurulmaz. Yorum artışı ve görünür sinyaller momentum olarak yorumlanır."
  };
  const item = {...p, id:productId, score, decision, momentum_label:momentum, evidence, first_seen:null, last_seen:null};
  if (pool && dbReady) {
    await dbQuery(`INSERT INTO discovered_products
      (id,user_id,source,product_url,title,brand,seller,image,first_seen,last_seen,current_price,rating_value,review_count,visible_sales_signal,exact_sales_count,score,decision,momentum_label,categories,evidence)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NOW(),NOW(),$9,$10,$11,$12,NULL,$13,$14,$15,$16::jsonb,$17::jsonb)
      ON CONFLICT (id) DO UPDATE SET
        last_seen=NOW(), product_url=EXCLUDED.product_url, title=EXCLUDED.title, brand=EXCLUDED.brand, seller=EXCLUDED.seller,
        image=EXCLUDED.image, current_price=EXCLUDED.current_price, rating_value=EXCLUDED.rating_value,
        review_count=EXCLUDED.review_count, score=EXCLUDED.score, decision=EXCLUDED.decision,
        momentum_label=EXCLUDED.momentum_label, evidence=EXCLUDED.evidence`,
      [productId,userId,p.source,p.product_url,p.title,p.brand,p.seller,p.image,p.current_price,p.rating_value,p.review_count,p.visible_sales_signal,score,decision,momentum,JSON.stringify(p.categories||[]),JSON.stringify(evidence)]);
    await dbQuery(`INSERT INTO product_snapshots(id,product_id,user_id,source,price,rating_value,review_count,seller,image,raw) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)`,
      [id("snap"),productId,userId,p.source,p.current_price,p.rating_value,p.review_count,p.seller,p.image,JSON.stringify(p.raw||{})]);
    if (delta.is_new || score >= 70 || (delta.review_delta || 0) > 0) {
      const type = delta.is_new ? "new_product" : (delta.review_delta || 0) > 0 ? "momentum" : "high_score";
      const sev = score >= 82 ? "high" : score >= 70 ? "medium" : "info";
      await dbQuery(`INSERT INTO momentum_events(id,product_id,user_id,source,event_type,severity,message,delta) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
        [id("momentum"),productId,userId,p.source,type,sev,`${p.title || productId}: ${momentum}`,JSON.stringify(delta)]);
      await dbQuery(`INSERT INTO alerts(id,user_id,product_id,alert_type,severity,title,message,payload) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)`,
        [id("alert"),userId,productId,type,sev,p.title || "Ürün adayı",`${momentum} • skor ${score}`,JSON.stringify({product:item,delta,run_id:runId})]);
    }
  }
  item.delta = delta;
  item.first_seen = delta.is_new ? now() : undefined;
  item.last_seen = now();
  return item;
}



async function runRadar(userId, params){
  const query = safeText(params.query || params.q || "okul çantası");
  const limit = Math.max(1, Math.min(50, Number(params.limit || 20)));
  const pages = Math.max(1, Math.min(3, Number(params.pages || 1)));
  const runId = id("run");
  const diagnostics = {
    strategy:"dedicated_trendyol_actor_fast_no_slow_fallback_default",
    api_pages:[], html_pages:[], apify_pages:[], apify_runs:[], product_page_errors:[],
    query_variants:queryVariants(query),
    provider_status:{apify_token_present:!!CONFIG.apifyToken, apify_actor_id:CONFIG.apifyActorId, apify_trendyol_actor_id:CONFIG.apifyTrendyolActorId}
  };
  if (pool && dbReady) await dbQuery(`INSERT INTO scan_runs(id,user_id,source,query,status,params) VALUES($1,$2,$3,$4,$5,$6::jsonb)`, [runId,userId,"Trendyol",query,"running",JSON.stringify(params)]);
  const seen = new Set();
  const collected = [];
  const addProducts = (products) => {
    for (const p of products || []) {
      const pid = p.product_id || extractContentIdFromUrl(p.product_url);
      if (!pid || seen.has(pid)) continue;
      seen.add(pid);
      collected.push(p);
      if (collected.length >= limit) break;
    }
  };
  let error = "";
  try {
    // 0) Dedicated Trendyol actor attempt
    for (const qv of diagnostics.query_variants) {
      if (collected.length < limit) {
        try { addProducts(await runDedicatedTrendyolActor(qv, limit - collected.length, params, diagnostics)); }
        catch(e) {
          diagnostics.dedicated_actor_runs = diagnostics.dedicated_actor_runs || [];
          diagnostics.dedicated_actor_runs.push({query:qv, error:e?.message || "dedicated_actor_error"});
        }
      }
      if (collected.length >= limit) break;
    }

    // M4.1.10: Do not fall into slow old Trendyol HTML/browser scraping by default.
    // It causes long waits and ends at /en/select-country. Dedicated actor is the working path.
    const allowSlowFallback = (
      CONFIG.radarSlowFallbackEnabled ||
      String(params.allowSlowFallback || params.slowFallback || params.debugSlow || "").toLowerCase() === "true" ||
      String(params.allowSlowFallback || params.slowFallback || params.debugSlow || "") === "1"
    );
    if (!allowSlowFallback && collected.length === 0) {
      diagnostics.fast_no_slow_fallback = true;
      diagnostics.fast_no_slow_fallback_reason = "Dedicated Trendyol actor failed/returned zero; skipped direct API/HTML/apify-web-scraper fallbacks because they are slow and mostly blocked.";
    }

    if (!allowSlowFallback && collected.length === 0) {
      // Skip old fallbacks.
    } else {

    // 1) Fast direct API attempt
    for (const qv of diagnostics.query_variants) {
      for (let page=1; page<=pages && collected.length<limit; page++) {
        try { addProducts(await collectTrendyolFromApi(qv, page, diagnostics)); }
        catch(e) { diagnostics.api_pages.push({query:qv, page, error:e?.message || "api_error"}); }
      }
      if (collected.length >= limit) break;
    }

    // 2) HTML search fallback
    if (collected.length === 0) {
      for (const qv of diagnostics.query_variants) {
        for (let page=1; page<=pages && collected.length<limit; page++) {
          try { addProducts(await collectTrendyolFromHtmlFallback(qv, page, limit - collected.length, diagnostics)); }
          catch(e) { diagnostics.html_pages.push({query:qv, page, error:e?.message || "html_error"}); }
        }
        if (collected.length >= limit) break;
      }
    }

    // 3) Browser/proxy provider fallback: Apify
    if (collected.length === 0) {
      for (const qv of diagnostics.query_variants) {
        for (let page=1; page<=pages && collected.length<limit; page++) {
          try { addProducts(await collectTrendyolFromApify(qv, page, limit - collected.length, diagnostics)); }
          catch(e) { diagnostics.apify_pages.push({query:qv, page, error:e?.message || "apify_error"}); }
        }
        if (collected.length >= limit) break;
      }
    }

    }

    const items = [];
    for (const p of collected.slice(0, limit)) items.push(await storeDiscoveryProduct(p, userId, runId, params));
    items.sort((a,b) => (b.score||0) - (a.score||0));
    if (pool && dbReady) await dbQuery(`UPDATE scan_runs SET status='success', finished_at=NOW(), found_count=$1, saved_count=$2 WHERE id=$3`, [collected.length,items.length,runId]);

    const warnings = [
      "Exact satış sayısı üretilmedi.",
      "Radar önce hızlı çalışan özel Trendyol actorünü dener; ürün bulursa yavaş fallbacklere geçmez.",
      "Shopify/Ads/Alibaba/Yerli adapterları canlı sağlayıcı bağlanınca aktif veri toplar."
    ];
    if (!CONFIG.apifyToken && items.length === 0) warnings.push("APIFY_TOKEN eklenmediği için browser/proxy fallback çalışmadı.");

    return {
      ok:true,
      mode:"cloud_autopilot_discovery",
      run_id:runId,
      source:"Trendyol",
      query,
      count:items.length,
      items,
      provider_required: items.length === 0 && !CONFIG.apifyToken ? {provider:"Apify", variable:"APIFY_TOKEN", reason:"Trendyol Railway IP'den arama/kategori taramasını 403 ile engelledi."} : null,
      diagnostics,
      warnings
    };
  } catch(e) {
    error = e?.message || "radar_error";
    if (pool && dbReady) await dbQuery(`UPDATE scan_runs SET status='error', finished_at=NOW(), error=$1 WHERE id=$2`, [error,runId]);
    return {ok:false, mode:"cloud_autopilot_discovery", run_id:runId, source:"Trendyol", query, error, items:[], diagnostics};
  }
}



async function listOpportunities(userId, limit=50){
  if (pool && dbReady) {
    const r = await dbQuery(`SELECT id, source, product_url, title, brand, seller, image, first_seen, last_seen, current_price, rating_value, review_count, visible_sales_signal, exact_sales_count, score, decision, momentum_label, evidence, ai_council FROM discovered_products WHERE user_id=$1 ORDER BY score DESC, last_seen DESC LIMIT $2`, [userId,limit]);
    return {ok:true, source:"postgres", count:r.rows.length, items:r.rows};
  }
  return {ok:true, source:"memory", count:0, items:[]};
}

async function listAlertsM4(userId, limit=50){
  if (pool && dbReady) {
    const r = await dbQuery(`SELECT id, created_at, product_id, alert_type, severity, title, message, seen, payload FROM alerts WHERE user_id=$1 ORDER BY created_at DESC LIMIT $2`, [userId,limit]);
    return {ok:true, source:"postgres", count:r.rows.length, items:r.rows};
  }
  return {ok:true, source:"memory", count:0, items:[]};
}

async function dashboardM4(userId){
  if (!pool || !dbReady) return {ok:true, version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true, radar_find_fix:true, browser_radar_apify:true, apify_link_extract_fix:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, counts:{db:false}};
  const q = async(sql,params=[]) => Number((await dbQuery(sql,params)).rows[0]?.count || 0);
  return {ok:true, app:CONFIG.app, version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true, radar_find_fix:true, browser_radar_apify:true, apify_link_extract_fix:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, counts:{
    discovered_products: await q(`SELECT COUNT(*) FROM discovered_products WHERE user_id=$1`,[userId]),
    alerts: await q(`SELECT COUNT(*) FROM alerts WHERE user_id=$1`,[userId]),
    saved_products: await q(`SELECT COUNT(*) FROM saved_products WHERE user_id=$1`,[userId]),
    scan_runs: await q(`SELECT COUNT(*) FROM scan_runs WHERE user_id=$1`,[userId]),
    high_score: await q(`SELECT COUNT(*) FROM discovered_products WHERE user_id=$1 AND score>=70`,[userId])
  }, env:{openai:!!CONFIG.openaiKey, db_ready:dbReady, trend_yol:true, radar_find_fix:true, browser_radar_apify:true, apify_link_extract_fix:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, apify_token_present:!!CONFIG.apifyToken}};
}

async function sourceHealthM4(){
  if (pool && dbReady) {
    const r = await dbQuery(`SELECT source,status,health,last_checked_at,last_error,config FROM source_registry ORDER BY source`);
    return {ok:true, source:"postgres", items:r.rows};
  }
  return {ok:true, items:[{source:"Trendyol",status:"configured",health:"unknown"}]};
}

async function saveProfileM4(userId, body){
  if (!pool || !dbReady) return {ok:false,error:"db_not_ready"};
  await dbQuery(`INSERT INTO user_profiles(user_id,budget,min_margin,max_sellers,min_rating,risk_level,sourcing_preference,categories,preferences)
    VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb)
    ON CONFLICT(user_id) DO UPDATE SET updated_at=NOW(), budget=EXCLUDED.budget, min_margin=EXCLUDED.min_margin, max_sellers=EXCLUDED.max_sellers, min_rating=EXCLUDED.min_rating, risk_level=EXCLUDED.risk_level, sourcing_preference=EXCLUDED.sourcing_preference, categories=EXCLUDED.categories, preferences=EXCLUDED.preferences`,
    [userId, body.budget || null, body.min_margin || body.minMargin || null, body.max_sellers || body.maxSellers || null, body.min_rating || body.minRating || null, safeText(body.risk_level || body.riskLevel || "medium"), safeText(body.sourcing_preference || body.sourcingPreference || "hybrid"), JSON.stringify(body.categories || []), JSON.stringify(body.preferences || body)]);
  return {ok:true,user_id:userId};
}

async function autopilotTickM4(userId){
  let categories = ["okul çantası", "ahşap oyuncak", "kamp lambası"];
  if (pool && dbReady) {
    const r = await dbQuery(`SELECT categories FROM user_profiles WHERE user_id=$1`, [userId]);
    if (Array.isArray(r.rows[0]?.categories) && r.rows[0].categories.length) categories = r.rows[0].categories;
  }
  const runs = [];
  for (const cat of categories.slice(0,4)) runs.push(await runRadar(userId, {query:cat, limit:12, pages:1, autopilot:true}));
  return {ok:true, mode:"autopilot_tick", user_id:userId, categories, runs};
}

function status(){
  return {
    ok:true,
    app:CONFIG.app,
    version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true,
    time:now(),
    uptime_seconds:Math.round(process.uptime()),
    milestone:"M4_1_FAST_LIMIT_NO_SLOW_FALLBACK_FIX",
    endpoints:{
      status:"GET /",
      ai_room:"GET/POST /ai-room",
      scan:"GET/POST /scan",
      product_url:"GET/POST /product-url",
      history:"GET /history",
      search:"GET /search?q=...",
      save:"POST /save",
      saved:"GET /saved",
      decision:"POST /decision",
      decisions:"GET /decisions",
      db_test:"GET /db-test",
      dashboard:"GET /dashboard",
      feature_matrix:"GET /feature-matrix",
      modules_status:"GET /modules/status",
      alibaba_sourcing:"GET/POST /sourcing/alibaba",
      local_production:"GET/POST /production/local",
      cost_roi:"GET/POST /cost/roi",
      meta_ads:"GET/POST /ads/meta",
      account_plan:"GET /account/plan",
      owner_dashboard:"GET /owner/dashboard",
      radar_run:"GET/POST /radar/run",
      radar_quick:"GET /radar/quick",
      radar_quick_dedicated:"GET /radar/quick-dedicated",
      autopilot_tick:"GET/POST /autopilot/tick",
      opportunities:"GET /opportunities",
      alerts:"GET /alerts",
      source_health:"GET /source-health",
      browser_radar_test:"GET /browser-radar/test",
      adapters:"GET /adapters"
    },
    env:{
      openai:!!CONFIG.openaiKey,
      openai_model:CONFIG.openaiModel,
      clean_ai_json:true,
      link_fetch_enabled:CONFIG.linkFetchEnabled,
      product_image_cleanup:true,
      trendyol_link_adapter:true,
      shopify_link_adapter:true,
      database_url_present:!!CONFIG.databaseUrl,
      db_ready:dbReady,
      db_error:dbError,
      api_token_required:!!CONFIG.apiToken,
      full_scope_loaded:true,
      auto_product_discovery:true,
      radar_find_fix:true,
      browser_radar_apify:true, apify_link_extract_fix:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true, usable_product_hunter:true,
      apify_token_present:!!CONFIG.apifyToken,
      apify_actor_id:CONFIG.apifyActorId, apify_trendyol_actor_id:CONFIG.apifyTrendyolActorId,
      cloud_autopilot_worker:true,
      opportunity_engine:true,
      momentum_engine:true,
      alerts_engine:true,
      serpapi_next:!!CONFIG.serpapiKey,
      apify_next:!!CONFIG.apifyToken,
      apify_link_extract_fix:true, apify_dom_extract_fix:true, dedicated_trendyol_actor:true, normalize_product_fields_fix:true, actor_schema_and_seller_fix:true, fast_radar_actor_priority_fix:true, fast_limit_no_slow_fallback_fix:true
    },
    policy:{
      exact_sales_count:"never_hallucinate",
      visible_sales_signal:"store_as_signal_not_exact_sales",
      evidence_gate:"lock_decision_if_critical_evidence_missing",
      ads_policy:"ad_intensity_is_not_sales"
    },
    memory_counts:{
      analyses:memory.analyses.length,
      saved:memory.saved.length,
      decisions:memory.decisions.length
    }
  };
}

function adaptersStatus(){
  return {
    ok:true,
    version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true,
    adapters:{
      trendyol_link:{
        status:"active",
        type:"public_page_evidence",
        extracts:["title","description","product_images","price_signal","rating_signal","review_count_signal","visible_sales_signal","brand_hint","seller_hint"],
        limitations:["review_text_not_yet","seller_competition_not_yet","exact_sales_never_generated"]
      },
      shopify_link:{
        status:"active",
        type:"public_page_evidence",
        extracts:["og_title","og_description","json_ld_product","images","price_signal","rating_if_available"],
        limitations:["store_traffic_not_yet","ad_creative_not_yet"]
      },
      alibaba:{
        status:"planned",
        extracts:["supplier","moq","unit_price","shipping","sample","lead_time","qc"],
        limitations:["not_active_in_m2_final"]
      },
      comments:{
        status:"planned",
        extracts:["positive_reviews","negative_reviews","photo_reviews","video_reviews"],
        limitations:["not_active_in_m2_final"]
      }
    }
  };
}


/* -------------------- M4.3 Module Infrastructure -------------------- */

function bodyProduct(body){
  const p = body?.product && typeof body.product === "object" ? body.product : {};
  return p;
}

function bodyProductTitle(body){
  const p = bodyProduct(body);
  return safeText(body?.query || body?.title || p.title || p.product_title || p.name || body?.message || "");
}

function bodyProductId(body){
  const p = bodyProduct(body);
  return safeText(body?.product_id || p.product_id || p.id || p.content_id || "");
}

function money(v){
  if (v === null || v === undefined || v === "") return null;
  if (typeof v === "number" && Number.isFinite(v)) return v;
  let s = String(v).replace("TL","").replace("₺","").replace("%","").replace(/[^0-9,\.\-]/g,"").trim();
  if (!s) return null;
  if (s.includes(",")) s = s.replace(/\./g,"").replace(",",".");
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

function detectProduction(title){
  const t = safeText(title).toLowerCase();
  const methods = [];
  const materials = [];
  if (/ahşap|wood|mdf|sunta/.test(t)) { materials.push("ahşap/MDF"); methods.push("CNC/lazer kesim", "zımpara/boya/vernik", "atölye numunesi"); }
  if (/kumaş|tekstil|çanta|bez|çorap|kılıf|organizer/.test(t)) { materials.push("kumaş/tekstil"); methods.push("dikiş atölyesi", "kesim-kalıp", "etiket/ambalaj"); }
  if (/plastik|silikon|abs|robot|oyuncak/.test(t)) { materials.push("plastik/silikon/elektronik"); methods.push("kalıp/ithalat daha olası", "CE/EN71/elektronik güvenlik kontrolü"); }
  if (/metal|paslanmaz|alüminyum|çelik/.test(t)) { materials.push("metal"); methods.push("sac/tel büküm", "lazer kesim", "kaplama/paketleme"); }
  if (!materials.length) { materials.push("belirsiz"); methods.push("malzeme doğrulaması", "yerli atölye numune teklifi"); }
  const localFit = materials.includes("kumaş/tekstil") || materials.includes("ahşap/MDF") || materials.includes("metal") ? "orta-yüksek" : "düşük-orta";
  return {materials:[...new Set(materials)], methods:[...new Set(methods)], local_fit:localFit};
}

async function moduleEvent(userId, type, title, payload){
  if (!pool || !dbReady) return;
  try {
    await dbQuery(`INSERT INTO owner_events(id,user_id,event_type,title,payload) VALUES($1,$2,$3,$4,$5::jsonb)`,
      [id("evt"), userId, type, title, JSON.stringify(payload || {})]);
  } catch(e) {}
}

async function alibabaModule(userId, body){
  const productTitle = bodyProductTitle(body);
  const productId = bodyProductId(body);
  const manualUnit = money(body.unit_price || body.unitCost);
  const manualMoq = safeInt(body.moq, null);
  const supplier = safeText(body.supplier_name || body.supplier || "");

  const saved = [];
  if (pool && dbReady && (supplier || manualUnit || manualMoq)) {
    try {
      await dbQuery(`INSERT INTO sourcing_offers(id,user_id,product_id,source,supplier_name,unit_price,currency,moq,lead_time,oem,private_label,confidence,url,raw)
        VALUES($1,$2,$3,'Alibaba/manual',$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb)`,
        [id("offer"), userId, productId || null, supplier || null, manualUnit, safeText(body.currency || "USD") || "USD", manualMoq,
        safeText(body.lead_time || ""), !!body.oem, !!body.private_label, manualUnit ? 0.65 : 0.35, safeText(body.url || ""), JSON.stringify(body)]);
      saved.push("manual_offer_saved");
    } catch(e) { saved.push("offer_save_failed:" + e.message); }
  }

  let offers = [];
  if (pool && dbReady && productId) {
    try {
      const r = await dbQuery(`SELECT supplier_name, unit_price, currency, moq, lead_time, confidence, url, created_at FROM sourcing_offers WHERE user_id=$1 AND product_id=$2 ORDER BY created_at DESC LIMIT 10`, [userId, productId]);
      offers = r.rows;
    } catch(e) {}
  }

  const locked = offers.length === 0 && !manualUnit;
  const result = {
    product_title: productTitle || "ürün seçilmedi",
    product_id: productId || null,
    live_provider_connected: false,
    supplier_offers_found: offers.length,
    cost_decision_locked: locked,
    offers
  };

  const resp = {
    ok:true,
    module:"Alibaba Sourcing",
    status:"adapter_ready",
    summary:"Tedarikçi teklif yapısı hazır. Canlı Alibaba sağlayıcısı bağlanmadan birim fiyat/MOQ uydurulmaz.",
    required_provider:"Alibaba search provider/API veya manuel teklif girişi",
    locked,
    quote_schema:["supplier_name","unit_price","currency","moq","sample_price","lead_time","shipping_volume","certificates","qc_note","url"],
    result,
    saved,
    missing_fields: locked ? ["Canlı tedarikçi teklifi", "MOQ", "birim fiyat", "numune fiyatı", "navlun/paket hacmi"] : [],
    next_actions:["3-5 tedarikçiden teklif topla", "Numune fiyatı ve MOQ iste", "Paket ölçüsü/ağırlık al", "QC ve sertifika kanıtı ekle"]
  };
  await moduleEvent(userId, "alibaba_module", productTitle, resp);
  return resp;
}

async function localProductionModule(userId, body){
  const productTitle = bodyProductTitle(body);
  const productId = bodyProductId(body);
  const fit = detectProduction(productTitle);
  const supplier = safeText(body.supplier_name || body.workshop || "");
  const unit = money(body.unit_cost || body.unit_price);
  const sample = money(body.sample_cost);
  const moq = safeInt(body.moq, null);

  if (pool && dbReady && (supplier || unit || sample || moq)) {
    try {
      await dbQuery(`INSERT INTO local_production_quotes(id,user_id,product_id,product_title,production_type,supplier_name,city,sample_cost,unit_cost,moq,lead_time,evidence)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb)`,
        [id("local"), userId, productId || null, productTitle, fit.materials.join(","), supplier || null, safeText(body.city || ""), sample, unit, moq, safeText(body.lead_time || ""), JSON.stringify(body)]);
    } catch(e) {}
  }

  const resp = {
    ok:true,
    module:"Yerli Üretim",
    status:"adapter_ready",
    summary:"Ürün yerli üretim ihtimali açısından sınıflandırıldı. Kesin fiyat için gerçek atölye teklifi gerekir.",
    locked: !unit,
    result:{
      product_title:productTitle || "ürün seçilmedi",
      product_id:productId || null,
      local_fit:fit.local_fit,
      possible_materials:fit.materials,
      production_methods:fit.methods,
      manual_quote_entered:!!unit
    },
    missing_fields: unit ? [] : ["Atölye adı", "numune maliyeti", "birim maliyet", "MOQ", "termin"],
    quote_schema:["supplier_name","city","sample_cost","unit_cost","moq","lead_time","material","production_method","qc_note"],
    next_actions:["Malzemeyi netleştir", "2 yerli atölyeden numune teklifi al", "Kalıp/dikiş/CNC/lazer uygunluğunu doğrula", "Ambalaj ve etiket maliyetini ekle"]
  };
  await moduleEvent(userId, "local_production_module", productTitle, resp);
  return resp;
}

async function costRoiModule(userId, body){
  const productTitle = bodyProductTitle(body);
  const productId = bodyProductId(body);
  const sale = money(body.sale_price || body.salePrice);
  const unit = money(body.unit_cost || body.unitCost);
  const commissionPct = money(body.commission_pct || body.commissionPct);
  const ship = money(body.shipping_packaging || body.shipping || body.cargo || 0) || 0;
  const ads = money(body.ads_cost || body.ads || 0) || 0;
  const tax = money(body.tax_cost || body.tax || 0) || 0;
  const ret = money(body.return_reserve || body.returnReserve || 0) || 0;

  const missing = [];
  if (sale === null) missing.push("sale_price");
  if (unit === null) missing.push("unit_cost");
  if (commissionPct === null) missing.push("commission_pct");
  const locked = missing.length > 0;

  let calc = null;
  if (!locked) {
    const commission = sale * (commissionPct / 100);
    const totalCost = unit + commission + ship + ads + tax + ret;
    const net = sale - totalCost;
    calc = {
      sale_price:Number(sale.toFixed(2)),
      unit_cost:Number(unit.toFixed(2)),
      commission_cost:Number(commission.toFixed(2)),
      shipping_packaging:Number(ship.toFixed(2)),
      ads_cost:Number(ads.toFixed(2)),
      tax_cost:Number(tax.toFixed(2)),
      return_reserve:Number(ret.toFixed(2)),
      total_cost:Number(totalCost.toFixed(2)),
      net_profit:Number(net.toFixed(2)),
      net_margin_pct:Number(((net / sale) * 100).toFixed(2)),
      roi_pct:Number(unit > 0 ? ((net / unit) * 100).toFixed(2) : 0)
    };
  }

  if (pool && dbReady) {
    try {
      await dbQuery(`INSERT INTO cost_calculations(id,user_id,product_id,product_title,sale_price,unit_cost,commission_pct,shipping_packaging,ads_cost,tax_cost,return_reserve,net_profit,net_margin,roi,locked,missing_fields,raw)
        VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16::jsonb,$17::jsonb)`,
        [id("cost"), userId, productId || null, productTitle, sale, unit, commissionPct, ship, ads, tax, ret, calc?.net_profit ?? null, calc?.net_margin_pct ?? null, calc?.roi_pct ?? null, locked, JSON.stringify(missing), JSON.stringify(body)]);
    } catch(e) {}
  }

  const resp = {
    ok:true,
    module:"Maliyet / ROI",
    status: locked ? "locked_missing_cost_evidence" : "calculated",
    summary: locked ? "Net kâr hesaplanmadı; gerekli maliyet kanıtları eksik." : "Net kâr ve ROI girilen maliyet kanıtlarına göre hesaplandı.",
    locked,
    missing_fields:missing,
    calculation:calc,
    next_actions: locked ? ["Ürün maliyetini gir", "Komisyon yüzdesini gir", "Kargo/paketleme ve reklam payını ekle"] : ["Tedarik fiyatını doğrula", "İade oranı ve reklam maliyetini sahada test et", "Kâr düşükse PASS/İZLE kararına çek"]
  };
  await moduleEvent(userId, "cost_roi_module", productTitle, resp);
  return resp;
}

async function metaAdsModule(userId, body){
  const productTitle = bodyProductTitle(body);
  const productId = bodyProductId(body);
  const creativeCount = safeInt(body.creative_count || body.creatives || null, null);
  const activePages = safeInt(body.active_pages || body.pages || null, null);
  const hasManual = creativeCount !== null || activePages !== null;
  let signal = "provider_required";
  if (hasManual) {
    const c = creativeCount || 0, p = activePages || 0;
    signal = c >= 20 || p >= 5 ? "yüksek reklam hareketi" : c >= 5 || p >= 2 ? "orta reklam hareketi" : "düşük reklam hareketi";
  }

  if (pool && dbReady && hasManual) {
    try {
      await dbQuery(`INSERT INTO ad_signals(id,user_id,product_id,product_title,platform,creative_count,active_pages,signal_label,confidence,evidence)
        VALUES($1,$2,$3,$4,'Meta Ads',$5,$6,$7,$8,$9::jsonb)`,
        [id("ads"), userId, productId || null, productTitle, creativeCount, activePages, signal, 0.55, JSON.stringify(body)]);
    } catch(e) {}
  }

  const resp = {
    ok:true,
    module:"Meta / Instagram Ads",
    status: hasManual ? "manual_signal_recorded" : "adapter_ready",
    summary:"Reklam yoğunluğu satış sayısı değildir. Bu modül reklam hareketini momentum sinyali olarak saklar.",
    required_provider: hasManual ? null : "Meta Ad Library/API veya browser worker",
    locked: !hasManual,
    signals: hasManual ? [signal] : [],
    result:{
      product_title:productTitle || "ürün seçilmedi",
      product_id:productId || null,
      creative_count:creativeCount,
      active_pages:activePages,
      exact_sales_count:null,
      sales_count_generated:false
    },
    missing_fields: hasManual ? [] : ["creative_count", "active_pages", "ad_start_dates", "page_names", "creative_urls"],
    next_actions:["Aynı kreatifi kaç sayfa kullanıyor ölç", "Yeni reklam açılış tarihini izle", "Trendyol/Shopify talep sinyaliyle çaprazla"]
  };
  await moduleEvent(userId, "meta_ads_module", productTitle, resp);
  return resp;
}

async function accountPlanModule(userId){
  if (pool && dbReady) {
    try {
      await dbQuery(`INSERT INTO saas_accounts(user_id,plan) VALUES($1,'builder_beta') ON CONFLICT(user_id) DO NOTHING`, [userId]);
      const r = await dbQuery(`SELECT user_id, plan, daily_scan_limit, ai_analysis_limit, saved_product_limit, created_at FROM saas_accounts WHERE user_id=$1`, [userId]);
      const row = r.rows[0];
      return {
        ok:true,
        module:"SaaS / Auth / Plan",
        status:"schema_ready",
        summary:"Satılabilir yapı için kullanıcı planı, kota ve limit şeması hazır. Ödeme sağlayıcısı sonraki aşama.",
        result:row,
        modules:["daily_scan_limit","ai_analysis_limit","saved_product_limit","api_token_ready","payment_provider_pending"],
        next_actions:["Gerçek login sağlayıcısı bağla", "Plan limitlerini panele taşı", "Ödeme sağlayıcısı seç", "Kota aşım davranışını belirle"]
      };
    } catch(e) {}
  }
  return {
    ok:true,
    module:"SaaS / Auth / Plan",
    status:"schema_ready_memory",
    summary:"Plan/kota yapısı hazır; Postgres bağlantısı yoksa kalıcı kayıt sınırlı.",
    result:{user_id:userId, plan:"builder_beta", daily_scan_limit:30, ai_analysis_limit:100, saved_product_limit:500}
  };
}

async function ownerDashboardModule(userId){
  const counts = {};
  if (pool && dbReady) {
    const one = async (name, sql, params=[userId]) => {
      try { const r = await dbQuery(sql, params); counts[name] = Number(r.rows[0]?.count || 0); }
      catch(e) { counts[name] = null; }
    };
    await one("discovered_products", `SELECT COUNT(*) FROM discovered_products WHERE user_id=$1`);
    await one("saved_products", `SELECT COUNT(*) FROM saved_products WHERE user_id=$1`);
    await one("decisions", `SELECT COUNT(*) FROM decisions WHERE user_id=$1`);
    await one("alerts", `SELECT COUNT(*) FROM alerts WHERE user_id=$1`);
    await one("ai_analyses", `SELECT COUNT(*) FROM analyses WHERE user_id=$1`);
    await one("scan_runs", `SELECT COUNT(*) FROM scan_runs WHERE user_id=$1`);
    await one("sourcing_offers", `SELECT COUNT(*) FROM sourcing_offers WHERE user_id=$1`);
    await one("local_quotes", `SELECT COUNT(*) FROM local_production_quotes WHERE user_id=$1`);
    await one("cost_calculations", `SELECT COUNT(*) FROM cost_calculations WHERE user_id=$1`);
    await one("ad_signals", `SELECT COUNT(*) FROM ad_signals WHERE user_id=$1`);
  }

  return {
    ok:true,
    module:"Owner Intelligence",
    status:"partial_live",
    summary:"Yönetici istihbaratı hazır: keşfedilen ürün, kayıt, karar, alarm, maliyet ve modül olayları izlenebilir.",
    result:counts,
    modules:["product_interest_tracking","saved_product_counts","decision_counts","source_health","scan_history","module_events"],
    next_actions:["Hangi kullanıcı hangi ürünü kaydetti ekranı", "En çok kaydedilen ürünler", "Kaynak hata grafiği", "Abonelik/plan dönüşüm ekranı"]
  };
}

function moduleStatusM43(){
  const fm = featureMatrix();
  return {
    ok:true,
    version:CONFIG.version,
    status:"m4.3_module_infra",
    summary:"Alibaba, Yerli Üretim, Maliyet/ROI, Meta Ads, SaaS ve Owner altyapıları endpoint seviyesinde hazırlandı.",
    modules:fm.features || [],
    warnings:["Canlı sağlayıcı olmayan modüller sahte veri üretmez.", "Tedarik ve maliyet kanıtı olmadan kâr kararı kilitli kalır."]
  };
}


/* -------------------- Router -------------------- */

const server = http.createServer(async(req,res)=>{
  try{
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    if(req.method === "OPTIONS") return send(res,204,{ok:true});
    if(url.pathname === "/favicon.ico") { res.writeHead(204); return res.end(); }

    if(req.method === "GET" && ["/","/health","/ready"].includes(url.pathname)) return send(res,200,status());
    if(req.method === "GET" && url.pathname === "/adapters") return send(res,200,adaptersStatus());

    if(req.method === "GET" && url.pathname === "/db-test") {
      await dbInitPromise;
      if (!pool) return send(res,200,{ok:false,db_ready:false,error:"DATABASE_URL yok"});
      try {
        const r = await dbQuery("SELECT NOW() AS now, current_database() AS database, current_user AS user");
        return send(res,200,{ok:true,db_ready:dbReady,row:r.rows[0]});
      } catch(e) {
        return send(res,500,{ok:false,db_ready:false,error:e?.message || "db_test_error"});
      }
    }

    if(url.pathname === "/product-url"){
      const deny = checkAuth(req); if(deny) return send(res,401,deny);
      const body = req.method === "POST" ? await readBody(req) : {};
      const productUrl = safeText(body.url || body.productUrl || url.searchParams.get("url"));
      const evidence = await readProductEvidence(productUrl);
      return send(res, evidence.ok ? 200 : 422, {ok:evidence.ok, app:CONFIG.app, version:CONFIG.version,
    m43_module_infra:true,
    philosophy:"Çok satanı değil, bizim satabileceğimiz çok satanı bul.", m41_working_core:true, product_evidence:evidence});
    }

    if(["/ai-room","/scan","/api/scan/new"].includes(url.pathname)){
      const body = req.method === "POST" ? await readBody(req) : {};
      return analyze(req,res,body,url.searchParams,url);
    }

    if(url.pathname === "/" && req.method === "POST"){
      const body = await readBody(req);
      return analyze(req,res,body,url.searchParams,url);
    }

    if(url.pathname === "/history" && req.method === "GET") return listHistory(req,res,url);
    if(url.pathname === "/search" && req.method === "GET") return searchHistory(req,res,url);

    if(url.pathname === "/save" && req.method === "POST") {
      const body = await readBody(req);
      return saveProduct(req,res,body,url);
    }

    if(url.pathname === "/saved" && req.method === "GET") return listSaved(req,res,url);

    if(url.pathname === "/decision" && req.method === "POST") {
      const body = await readBody(req);
      return saveDecision(req,res,body,url);
    }

    if(url.pathname === "/decisions" && req.method === "GET") return listDecisions(req,res,url);


    if(req.method === "GET" && url.pathname === "/feature-matrix") return send(res,200,featureMatrix());
    if(req.method === "GET" && url.pathname === "/dashboard") return send(res,200,await dashboardM4(getUserId(req,url,{})));
    if(req.method === "GET" && url.pathname === "/source-health") return send(res,200,await sourceHealthM4());

    if(req.method === "GET" && (url.pathname === "/modules/status" || url.pathname === "/module-status")) return send(res,200,moduleStatusM43());

    if(url.pathname === "/sourcing/alibaba") {
      const body = req.method === "POST" ? await readBody(req) : Object.fromEntries(url.searchParams.entries());
      return send(res,200,await alibabaModule(getUserId(req,url,body), body));
    }

    if(url.pathname === "/production/local") {
      const body = req.method === "POST" ? await readBody(req) : Object.fromEntries(url.searchParams.entries());
      return send(res,200,await localProductionModule(getUserId(req,url,body), body));
    }

    if(url.pathname === "/cost/roi") {
      const body = req.method === "POST" ? await readBody(req) : Object.fromEntries(url.searchParams.entries());
      return send(res,200,await costRoiModule(getUserId(req,url,body), body));
    }

    if(url.pathname === "/ads/meta") {
      const body = req.method === "POST" ? await readBody(req) : Object.fromEntries(url.searchParams.entries());
      return send(res,200,await metaAdsModule(getUserId(req,url,body), body));
    }

    if(req.method === "GET" && url.pathname === "/account/plan") {
      return send(res,200,await accountPlanModule(getUserId(req,url,{})));
    }

    if(req.method === "GET" && url.pathname === "/owner/dashboard") {
      return send(res,200,await ownerDashboardModule(getUserId(req,url,{})));
    }



    
    if(url.pathname === "/dedicated-trendyol/test"){
      const query = safeText(url.searchParams.get("query") || url.searchParams.get("q") || "okul çantası");
      const limit = Math.max(1, Math.min(20, Number(url.searchParams.get("limit") || 10)));
      const diagnostics = {strategy:"dedicated_trendyol_actor_only", dedicated_actor_runs:[]};
      const items = await runDedicatedTrendyolActor(query, limit, Object.fromEntries(url.searchParams.entries()), diagnostics);
      return send(res,200,{ok:items.length>0,version:CONFIG.version,query,count:items.length,items,diagnostics,required:{APIFY_TOKEN:!CONFIG.apifyToken, APIFY_TRENDYOL_ACTOR_ID:false}});
    }

if(url.pathname === "/browser-radar/test") {
      const userId = CONFIG.defaultUserId;
      const q = safeText(url.searchParams.get("query") || "okul çantası");
      const page = Math.max(1, Math.min(3, Number(url.searchParams.get("page") || 1)));
      const diagnostics = {apify_pages:[], apify_runs:[], product_page_errors:[]};
      try {
        const items = await collectTrendyolFromApify(q, page, 5, diagnostics);
        return send(res,200,{ok:true, version:CONFIG.version, query:q, count:items.length, items, diagnostics});
      } catch(e) {
        return send(res,200,{ok:false, version:CONFIG.version, query:q, error:e?.message || "browser_radar_test_error", diagnostics, required:{APIFY_TOKEN:!CONFIG.apifyToken}});
      }
    }

    
    
    if(url.pathname === "/radar/quick-dedicated") {
      const q = url.searchParams.get("query") || url.searchParams.get("q") || "okul cantasi";
      const params = Object.fromEntries(url.searchParams.entries());
      const limit = Math.max(1, Math.min(10, Number(params.limit || 3)));
      const diagnostics = {
        strategy:"dedicated_trendyol_actor_only_fast",
        dedicated_actor_runs:[],
        query_variants:queryVariants(q),
        provider_status:{apify_token_present:!!CONFIG.apifyToken, apify_actor_id:CONFIG.apifyActorId, apify_trendyol_actor_id:CONFIG.apifyTrendyolActorId}
      };
      const items = [];
      const seen = new Set();
      for (const qv of diagnostics.query_variants) {
        const found = await runDedicatedTrendyolActor(qv, limit, params, diagnostics);
        for (const p of found) {
          const pid = p.product_id || extractContentIdFromUrl(p.product_url);
          if (!pid || seen.has(pid)) continue;
          seen.add(pid);
          items.push(p);
          if (items.length >= limit) break;
        }
        if (items.length >= limit) break;
      }
      return send(res,200,{ok:true,version:CONFIG.version,query:q,count:items.length,items:items.slice(0,limit),diagnostics});
    }

if(url.pathname === "/radar/quick") {
      const q = url.searchParams.get("query") || url.searchParams.get("q") || "okul cantasi";
      const params = Object.fromEntries(url.searchParams.entries());
      params.query = q;
      params.limit = Math.max(1, Math.min(5, Number(params.limit || 3)));
      params.pages = 1;
      const result = await runRadar("default", params);
      return send(res,200,result);
    }


    if(url.pathname === "/hunt/run") {
      const body = req.method === "POST" ? await readBody(req) : {};
      const params = Object.fromEntries(url.searchParams.entries());
      const userId = getUserId(req,url,body);
      const queryRaw = safeText(params.query || body.query || params.q || body.q || "ev düzenleyici, pratik mutfak");
      const seeds = queryRaw.split(",").map(x => safeText(x)).filter(Boolean).slice(0,2);
      const all = [];
      const runs = [];
      const seen = new Set();
      for (const q of seeds.length ? seeds : ["ev düzenleyici"]) {
        const result = await runRadar(userId, {...params, ...body, query:q, limit:Math.max(1, Math.min(4, Number(params.perQueryLimit || 4))), pages:1});
        runs.push({query:q, ok:result.ok, count:result.count, run_id:result.run_id});
        for (const item of result.items || []) {
          const pid = item.product_id || item.id || item.product_url || item.title;
          if (!pid || seen.has(pid)) continue;
          seen.add(pid);
          all.push(item);
        }
      }
      all.sort((a,b) => Number(b.score||0) - Number(a.score||0));
      return send(res,200,{ok:true, mode:"usable_product_hunter", version:CONFIG.version, count:all.length, items:all.slice(0,12), runs, warnings:["Çok ürün değil, çeşitlendirilmiş aday listesi. Exact satış sayısı uydurulmaz."]});
    }

if(url.pathname === "/radar/run") {
      const body = req.method === "POST" ? await readBody(req) : {};
      const params = Object.fromEntries(url.searchParams.entries());
      return send(res,200,await runRadar(getUserId(req,url,body), {...params, ...body}));
    }

    if(url.pathname === "/autopilot/tick") {
      const body = req.method === "POST" ? await readBody(req) : {};
      return send(res,200,await autopilotTickM4(getUserId(req,url,body)));
    }


    if(req.method === "GET" && (url.pathname === "/hunter/feed" || url.pathname === "/feed")) {
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get("limit") || 50)));
      const data = await listOpportunities(getUserId(req,url,{}), limit);
      return send(res,200,{...data, mode:"usable_product_hunter_feed", note:"Sade uygulama ekranı için fırsat adayları. Exact satış uydurulmaz."});
    }

    if(req.method === "GET" && url.pathname === "/opportunities") {
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get("limit") || 50)));
      return send(res,200,await listOpportunities(getUserId(req,url,{}), limit));
    }

    if(req.method === "GET" && url.pathname === "/alerts") {
      const limit = Math.max(1, Math.min(100, Number(url.searchParams.get("limit") || 50)));
      return send(res,200,await listAlertsM4(getUserId(req,url,{}), limit));
    }

    if(url.pathname === "/profile" && req.method === "POST") {
      const body = await readBody(req);
      return send(res,200,await saveProfileM4(getUserId(req,url,body), body));
    }

    return send(res,404,{ok:false,error:"endpoint_not_found",path:url.pathname});
  }catch(e){
    console.error("server_error",e);
    return send(res,500,{ok:false,error:"server_error",message:e?.message || "Bilinmeyen hata"});
  }
});

server.listen(PORT,HOST,()=>console.log(`Ürün Dedektifi API ${CONFIG.version} running on http://${HOST}:${PORT}`));
