import http from "node:http";
import { Pool } from "pg";

const HOST = "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

const CONFIG = {
  app: process.env.PUBLIC_API_NAME || "Ürün Dedektifi API",
  version: "2.3.0-m2-trendyol-link-adapter",
  openaiKey: process.env.OPENAI_API_KEY || "",
  openaiModel: process.env.OPENAI_MODEL || "gpt-5-mini",
  apiToken: process.env.API_TOKEN || "",
  databaseUrl: process.env.DATABASE_URL || "",
  serpapiKey: process.env.SERPAPI_KEY || "",
  apifyToken: process.env.APIFY_TOKEN || "",
  linkFetchEnabled: (process.env.LINK_FETCH_ENABLED || "true").toLowerCase() !== "false"
};

const memory = { analyses: [], saved: [], decisions: [], created_at: new Date().toISOString() };

function now(){ return new Date().toISOString(); }
function id(prefix){ return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2,8)}`; }
function safeText(v){ return v === null || v === undefined ? "" : String(v).trim(); }
function safeInt(v, fallback=0){ const n = Number(v); return Number.isFinite(n) ? Math.round(n) : fallback; }
function uniq(arr){ return [...new Set((arr || []).map(safeText).filter(Boolean))]; }

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

function checkAuth(req){
  if (!CONFIG.apiToken) return null;
  const auth = safeText(req.headers.authorization);
  const token = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : safeText(req.headers["x-api-token"]);
  return token === CONFIG.apiToken ? null : { ok:false, error:"unauthorized" };
}

/* -------------------- Public product link adapter -------------------- */

function detectSource(productUrl){
  const u = safeText(productUrl).toLowerCase();
  if (u.includes("shopify")) return "Shopify";
  if (u.includes("trendyol")) return "Trendyol";
  if (u.includes("etsy")) return "Etsy";
  if (u.includes("amazon")) return "Amazon";
  if (u.includes("alibaba")) return "Alibaba";
  return productUrl ? "Public Link" : "AI Oda";
}

function isHttpUrl(v){
  try {
    const u = new URL(v);
    return u.protocol === "http:" || u.protocol === "https:";
  } catch { return false; }
}

function decodeEntities(s){
  return safeText(s)
    .replace(/&quot;/g,'"')
    .replace(/&#34;/g,'"')
    .replace(/&#x27;/g,"'")
    .replace(/&#39;/g,"'")
    .replace(/&amp;/g,"&")
    .replace(/&lt;/g,"<")
    .replace(/&gt;/g,">")
    .replace(/\s+/g," ")
    .trim();
}

function stripTags(s){
  return decodeEntities(safeText(s).replace(/<script[\s\S]*?<\/script>/gi," ").replace(/<style[\s\S]*?<\/style>/gi," ").replace(/<[^>]+>/g," "));
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

function findImageUrls(html, source){
  const urls = [];
  const imgRe = /https?:\/\/[^"'<>\\\s]+\.(?:jpg|jpeg|png|webp)(?:\?[^"'<>\\\s]*)?/gi;
  let m;
  while ((m = imgRe.exec(html)) !== null) urls.push(m[0]);

  const cdnRe = /https?:\/\/[^"'<>\\\s]*dsmcdn\.com[^"'<>\\\s]*/gi;
  while ((m = cdnRe.exec(html)) !== null) {
    if (/\.(jpg|jpeg|png|webp)/i.test(m[0])) urls.push(m[0]);
  }

  return uniq(urls)
    .filter(u => !/logo|sprite|favicon/i.test(u))
    .slice(0, 12);
}

function extractPrice(html, product){
  const offer = Array.isArray(product?.offers) ? product.offers[0] : product?.offers;
  const p = offer?.price || offer?.lowPrice || offer?.highPrice || "";
  const currency = offer?.priceCurrency || "TRY";
  if (p) return { visible_price: String(p), currency, evidence: "json_ld_offer" };

  const candidates = [
    /"sellingPrice"\s*:\s*"?([0-9]+(?:[.,][0-9]+)?)"?/i,
    /"price"\s*:\s*"?([0-9]+(?:[.,][0-9]+)?)"?/i,
    /([0-9]{1,3}(?:\.[0-9]{3})*(?:,[0-9]{2})?)\s*TL/i
  ];
  for (const re of candidates) {
    const m = html.match(re);
    if (m?.[1]) return { visible_price: m[1], currency:"TRY", evidence:"html_regex" };
  }
  return null;
}

function extractRating(product){
  const rating = product?.aggregateRating;
  if (!rating) return null;
  return {
    rating_value: rating.ratingValue ? String(rating.ratingValue) : null,
    review_count: rating.reviewCount || rating.ratingCount ? Number(rating.reviewCount || rating.ratingCount) : null,
    evidence: "json_ld_aggregateRating"
  };
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

function extractSellerHint(html){
  const patterns = [
    /"merchantName"\s*:\s*"([^"]+)"/i,
    /"sellerName"\s*:\s*"([^"]+)"/i,
    /"supplierName"\s*:\s*"([^"]+)"/i,
    /"storeName"\s*:\s*"([^"]+)"/i
  ];
  for (const p of patterns) {
    const m = html.match(p);
    if (m?.[1]) return decodeEntities(m[1]);
  }
  return "";
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
    images:[],
    video:null,
    price:null,
    rating:null,
    seller_name:"",
    visible_sales_signal:null,
    exact_sales_count:null,
    reviews:[],
    review_status:"not_available",
    limitations:[],
    evidence_quality:"none"
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

    const title = flattenClean(productLd?.name || ogTitle || titleTag(html));
    const description = flattenClean(productLd?.description || ogDesc);
    const images = uniq([ogImage, ...imageListFromJsonLd(productLd), ...findImageUrls(html, source)]).slice(0, 12);
    const price = extractPrice(html, productLd);
    const rating = extractRating(productLd);
    const visibleSales = extractVisibleSalesSignal(html);
    const seller = extractSellerHint(html);

    const limitations = [];
    if (!images.length) limitations.push("Fotoğraf linki bulunamadı veya sayfa dinamik yüklüyor.");
    if (!rating) limitations.push("Puan/yorum sayısı bulunamadı.");
    if (!visibleSales) limitations.push("Görünür satış sinyali bulunamadı.");
    limitations.push("Yorum metinleri bu aşamada çekilmiyor; yorum adapterı M2.4/M2.5 aşamasında bağlanacak.");
    limitations.push("Exact satış sayısı üretilmedi.");

    return {
      ...base,
      ok:true,
      title,
      description,
      images,
      product_video:null,
      price,
      rating,
      seller_name:seller,
      visible_sales_signal:visibleSales,
      exact_sales_count:null,
      review_status:"pending_review_adapter",
      limitations,
      evidence_quality: [title, description, images.length, price, rating, visibleSales].filter(Boolean).length >= 3 ? "medium" : "low"
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

function flattenClean(v, max=900){
  if (v === null || v === undefined) return "";
  if (typeof v === "string") return v.replace(/\s+/g," ").trim().slice(0,max);
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
    alibaba_research:flattenClean(c.alibaba_research, 700) || extractString(txt,"alibaba_research") || "Canlı Alibaba araştırması M2 adapter ile yapılacak.",
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
  const extraMissing = productEvidence?.ok
    ? ["Yorum metinleri", "Satıcı yoğunluğu", "Canlı Alibaba maliyeti"]
    : fallbackMissing;
  return {
    mode:"local_fallback",
    real_ai:false,
    score: productEvidence?.ok ? 58 : 55,
    decision:"İNCELE",
    summary: productEvidence?.ok ? `Link okundu: ${productEvidence.title || productEvidence.source}. AI yoksa yerel ön analiz.` : "Yerel güvenli ön analiz. Canlı kaynaklar bağlanınca derin veriyle zenginleşir.",
    gpt:"Niş fırsat olabilir; hedef kitle ve kullanım senaryosu netleştirilmeli.",
    gemini:"Trend olumlu olabilir; sürdürülebilirlik/Montessori gibi açıları kreatif avantaj sağlar.",
    claude:"Kimyasal test, yaş grubu güvenliği, marka/IP ve iade riski kontrol edilmeden AL kilitli.",
    deepseek:"Alibaba canlı maliyet araştırması için adapter gerekir; MOQ, birim fiyat, navlun ve paket hacmi toplanmalı.",
    common_points:fallbackCommon,
    objections:["Canlı veri eksik","Regülasyon belirsiz"],
    missing_evidence:extraMissing,
    next_actions:["Alibaba adapter bağla","Trendyol/Shopify ürün görsellerini çek","Yorumları sınıflandır"],
    judge:"İNCELE; AL kararı Evidence Gate ile kilitli.",
    alibaba_research:"Canlı Alibaba araması M2'de bağlanacak.",
    product_strengths:["Niş hedef kitle olabilir"],
    review_insights:["Yorum çekimi bekliyor"],
    visual_insights: productEvidence?.images?.length ? [`${productEvidence.images.length} görsel linki yakalandı.`] : ["Foto/video çekimi bekliyor"]
  };
}

function buildAiPrompt(payload, productEvidence){
  return `
Sen Ürün Dedektifi AI Savaş Odası'sın. Cevabın SADECE geçerli JSON olacak.

Çok önemli format kuralları:
- gpt, gemini, claude, deepseek, judge, alibaba_research alanları STRING olacak. Obje veya array yapma.
- common_points, objections, missing_evidence, next_actions, product_strengths, review_insights, visual_insights alanları ARRAY OF STRING olacak.
- real_ai true olacak.
- mode "openai_war_room" olacak.
- decision sadece şunlardan biri olacak: GOLD, GÜÇLÜ ADAY, İNCELE, RİSKLİ, PASS
- Markdown ve kod bloğu yok.

Derinlik kuralı:
Her rol 3-5 cümlelik güçlü tartışma yazsın.
- gpt: ticari fırsat, niş, hedef kitle, ürünün güçlü yönleri
- gemini: trend, pazar, kreatif, sosyal medya, sürdürülebilirlik
- claude: acımasız risk itirazı, kalite, iade, regülasyon, marka/IP
- deepseek: Alibaba/tedarik/maliyet bakışı. Canlı Alibaba verisi yoksa "canlı veri yok" de; MOQ, birim fiyat, navlun, paket hacmi, numune, kalite kontrol, yerli üretim alternatifini tartış. Maliyet uydurma.

Kanıt kuralı:
Ürün fotoğrafı, yorum, video, satıcı sayısı, satış sinyali yoksa açıkça "kanıt yok" de.
Exact satış sayısı uydurma. Reklam yoğunluğu satış değildir.
Eğer ürün_link_kanıtı içinde fiyat/görsel/puan varsa bunu kanıt olarak kullan; yoksa uydurma.

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

ürün_link_kanıtı:
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

async function saveAnalysisToDb(result){
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
      "demo",
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

async function analyze(req,res,body,params){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);

  await dbInitPromise;
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
    created_at:now(),
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
      price:productEvidence?.price || null,
      rating:productEvidence?.rating || null,
      seller_name:productEvidence?.seller_name || "",
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
      live_alibaba:"Canlı Alibaba maliyeti M2.5 adapter ile bağlanacak."
    }
  };

  memory.analyses.unshift(result);
  memory.analyses = memory.analyses.slice(0,100);

  try {
    const saved = await saveAnalysisToDb(result);
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
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 30)));
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, message, product_url, source, score, decision, ai_council, evidence_gate, products, raw->'product_evidence' AS product_evidence
       FROM analyses
       WHERE user_id=$1
       ORDER BY created_at DESC
       LIMIT $2`,
      ["demo", limit]
    );
    return send(res,200,{ok:true,source:"postgres",count:r.rows.length,items:r.rows});
  }
  return send(res,200,{ok:true,source:"memory",count:memory.analyses.length,items:memory.analyses.slice(0,limit)});
}

async function saveProduct(req,res,body){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;

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
    user_id:"demo",
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
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 30)));
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, analysis_id, title, source, product_url, score, decision, product, notes
       FROM saved_products
       WHERE user_id=$1
       ORDER BY created_at DESC
       LIMIT $2`,
      ["demo", limit]
    );
    return send(res,200,{ok:true,source:"postgres",count:r.rows.length,items:r.rows});
  }
  return send(res,200,{ok:true,source:"memory",count:memory.saved.length,items:memory.saved.slice(0,limit)});
}

async function saveDecision(req,res,body){
  const deny = checkAuth(req); if(deny) return send(res,401,deny);
  await dbInitPromise;

  const item = {
    id:id("decision"),
    user_id:"demo",
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
  const limit = Math.min(100, Math.max(1, Number(url.searchParams.get("limit") || 30)));
  if (pool && dbReady) {
    const r = await dbQuery(
      `SELECT id, created_at, analysis_id, product_id, decision, notes, payload
       FROM decisions
       WHERE user_id=$1
       ORDER BY created_at DESC
       LIMIT $2`,
      ["demo", limit]
    );
    return send(res,200,{ok:true,source:"postgres",count:r.rows.length,items:r.rows});
  }
  return send(res,200,{ok:true,source:"memory",count:memory.decisions.length,items:memory.decisions.slice(0,limit)});
}

function status(){
  return {
    ok:true,
    app:CONFIG.app,
    version:CONFIG.version,
    time:now(),
    uptime_seconds:Math.round(process.uptime()),
    endpoints:{
      status:"GET /",
      ai_room:"GET/POST /ai-room",
      scan:"GET/POST /scan",
      product_url:"GET/POST /product-url",
      history:"GET /history",
      save:"POST /save",
      saved:"GET /saved",
      decision:"POST /decision",
      decisions:"GET /decisions",
      db_test:"GET /db-test"
    },
    env:{
      openai:!!CONFIG.openaiKey,
      openai_model:CONFIG.openaiModel,
      clean_ai_json:true,
      link_fetch_enabled:CONFIG.linkFetchEnabled,
      trendyol_link_adapter:true,
      database_url_present:!!CONFIG.databaseUrl,
      db_ready:dbReady,
      db_error:dbError,
      api_token_required:!!CONFIG.apiToken,
      serpapi_next:!!CONFIG.serpapiKey,
      apify_next:!!CONFIG.apifyToken
    },
    memory_counts:{
      analyses:memory.analyses.length,
      saved:memory.saved.length,
      decisions:memory.decisions.length
    }
  };
}

const server = http.createServer(async(req,res)=>{
  try{
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    if(req.method === "OPTIONS") return send(res,204,{ok:true});
    if(url.pathname === "/favicon.ico") { res.writeHead(204); return res.end(); }

    if(req.method === "GET" && ["/","/health","/ready"].includes(url.pathname)) return send(res,200,status());

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
      const body = req.method === "POST" ? await readBody(req) : {};
      const productUrl = safeText(body.url || body.productUrl || url.searchParams.get("url"));
      const evidence = await readProductEvidence(productUrl);
      return send(res, evidence.ok ? 200 : 422, {ok:evidence.ok, app:CONFIG.app, version:CONFIG.version, product_evidence:evidence});
    }

    if(["/ai-room","/scan","/api/scan/new"].includes(url.pathname)){
      const body = req.method === "POST" ? await readBody(req) : {};
      return analyze(req,res,body,url.searchParams);
    }

    if(url.pathname === "/" && req.method === "POST"){
      const body = await readBody(req);
      return analyze(req,res,body,url.searchParams);
    }

    if(url.pathname === "/history" && req.method === "GET") return listHistory(req,res,url);

    if(url.pathname === "/save" && req.method === "POST") {
      const body = await readBody(req);
      return saveProduct(req,res,body);
    }

    if(url.pathname === "/saved" && req.method === "GET") return listSaved(req,res,url);

    if(url.pathname === "/decision" && req.method === "POST") {
      const body = await readBody(req);
      return saveDecision(req,res,body);
    }

    if(url.pathname === "/decisions" && req.method === "GET") return listDecisions(req,res,url);

    return send(res,404,{ok:false,error:"endpoint_not_found",path:url.pathname});
  }catch(e){
    console.error("server_error",e);
    return send(res,500,{ok:false,error:"server_error",message:e?.message || "Bilinmeyen hata"});
  }
});

server.listen(PORT,HOST,()=>console.log(`Ürün Dedektifi API ${CONFIG.version} running on http://${HOST}:${PORT}`));
