import http from "node:http";
import { Pool } from "pg";

const HOST = "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

const CONFIG = {
  app: process.env.PUBLIC_API_NAME || "Ürün Dedektifi API",
  version: "2.1.0-m2-postgres-persistence",
  openaiKey: process.env.OPENAI_API_KEY || "",
  openaiModel: process.env.OPENAI_MODEL || "gpt-5-mini",
  apiToken: process.env.API_TOKEN || "",
  databaseUrl: process.env.DATABASE_URL || "",
  serpapiKey: process.env.SERPAPI_KEY || "",
  apifyToken: process.env.APIFY_TOKEN || ""
};

const memory = { analyses: [], saved: [], decisions: [], created_at: new Date().toISOString() };

function now(){ return new Date().toISOString(); }
function id(prefix){ return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2,8)}`; }
function safeText(v){ return v === null || v === undefined ? "" : String(v).trim(); }
function safeInt(v, fallback=0){ const n = Number(v); return Number.isFinite(n) ? Math.round(n) : fallback; }

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
function extractArray(src,key){
  const re = new RegExp(`"${key}"\\s*:\\s*\\[([\\s\\S]*?)\\]`, "s");
  const m = safeText(src).match(re);
  if(!m) return [];
  try { return JSON.parse(`[${m[1]}]`).map(safeText).filter(Boolean); } catch {}
  const vals=[]; const itemRe=/"((?:\\.|[^"\\])*)"/g; let im;
  while((im=itemRe.exec(m[1]))!==null){ try{vals.push(JSON.parse(`"${im[1]}"`));}catch{vals.push(im[1]);} }
  return vals.filter(Boolean);
}
function safeArray(v){ return Array.isArray(v) ? v.map(safeText).filter(Boolean) : []; }
function decisionFromScore(score){ return score>=82?"GOLD":score>=70?"GÜÇLÜ ADAY":score>=55?"İNCELE":score>=40?"RİSKLİ":"PASS"; }

function normalizeCouncil(raw, txt=""){
  const c = raw && typeof raw === "object" ? raw : {};
  const scoreNum = Number(c.score ?? extractNumber(txt,"score"));
  const score = Number.isFinite(scoreNum) ? Math.max(0,Math.min(100,Math.round(scoreNum))) : 50;
  const allowed = ["GOLD","GÜÇLÜ ADAY","İNCELE","RİSKLİ","PASS"];
  const ed = extractString(txt,"decision");
  const decision = allowed.includes(c.decision) ? c.decision : allowed.includes(ed) ? ed : decisionFromScore(score);

  return {
    mode:"openai_war_room",
    real_ai:true,
    summary:safeText(c.summary)||extractString(txt,"summary")||"Derin AI tartışması üretildi.",
    score, decision,
    gpt:safeText(c.gpt)||extractString(txt,"gpt")||"Ticari fırsat ve hedef kitle kanıtlarla tartışılmalı.",
    gemini:safeText(c.gemini)||extractString(txt,"gemini")||"Trend sinyalleri satış sayısı değildir; pazar sinyalleri doğrulanmalı.",
    claude:safeText(c.claude)||extractString(txt,"claude")||"Risk, regülasyon ve marka/IP kanıtı olmadan AL kilitli kalır.",
    deepseek:safeText(c.deepseek)||extractString(txt,"deepseek")||"Alibaba/tedarik maliyeti için canlı kaynak adapterı gerekir.",
    common_points:safeArray(c.common_points).length?safeArray(c.common_points):extractArray(txt,"common_points"),
    objections:safeArray(c.objections).length?safeArray(c.objections):extractArray(txt,"objections"),
    missing_evidence:safeArray(c.missing_evidence).length?safeArray(c.missing_evidence):extractArray(txt,"missing_evidence"),
    next_actions:safeArray(c.next_actions).length?safeArray(c.next_actions):extractArray(txt,"next_actions"),
    judge:safeText(c.judge)||extractString(txt,"judge")||"Kritik kanıtlar tamamlanmadan AL kararı kilitli.",
    alibaba_research:safeText(c.alibaba_research)||extractString(txt,"alibaba_research")||"Canlı Alibaba araştırması M2 adapter ile yapılacak.",
    product_strengths:safeArray(c.product_strengths).length?safeArray(c.product_strengths):extractArray(txt,"product_strengths"),
    review_insights:safeArray(c.review_insights).length?safeArray(c.review_insights):extractArray(txt,"review_insights"),
    visual_insights:safeArray(c.visual_insights).length?safeArray(c.visual_insights):extractArray(txt,"visual_insights")
  };
}

function localCouncil(){
  return {
    mode:"local_fallback",
    real_ai:false,
    score:55,
    decision:"İNCELE",
    summary:"Yerel güvenli ön analiz. Canlı kaynaklar bağlanınca derin veriyle zenginleşir.",
    gpt:"Niş fırsat olabilir; hedef kitle ve kullanım senaryosu netleştirilmeli.",
    gemini:"Trend olumlu olabilir; sürdürülebilirlik/Montessori gibi açıları kreatif avantaj sağlar.",
    claude:"Kimyasal test, yaş grubu güvenliği, marka/IP ve iade riski kontrol edilmeden AL kilitli.",
    deepseek:"Alibaba canlı maliyet araştırması için adapter gerekir; MOQ, birim fiyat, navlun ve paket hacmi toplanmalı.",
    common_points:["Kanıt olmadan winner denmez","Tedarik maliyeti şart","Satıcı yoğunluğu ölçülmeli"],
    objections:["Canlı veri eksik","Regülasyon belirsiz"],
    missing_evidence:["Canlı Trendyol/Shopify/Alibaba kanıtı","Ürün fotoğrafları/video","Olumlu/olumsuz yorum kanıtı","Satıcı yoğunluğu","GTIP/vergi/regülasyon","Marka/IP kontrolü"],
    next_actions:["Alibaba adapter bağla","Trendyol/Shopify ürün görsellerini çek","Yorumları sınıflandır"],
    judge:"İNCELE; AL kararı Evidence Gate ile kilitli.",
    alibaba_research:"Canlı Alibaba araması M2'de bağlanacak.",
    product_strengths:["Niş hedef kitle olabilir"],
    review_insights:["Yorum çekimi bekliyor"],
    visual_insights:["Foto/video çekimi bekliyor"]
  };
}

async function openAiCouncil(payload){
  if(!CONFIG.openaiKey) return localCouncil();
  const prompt = `
Sen Ürün Dedektifi AI Savaş Odası'sın. Tatmin edici ama JSON olarak cevap ver.

Roller:
- gpt: ticari fırsat, niş, hedef kitle, güçlü yönler
- gemini: trend, pazar, kreatif, sosyal medya, sürdürülebilirlik
- claude: acımasız risk itirazı, kalite, iade, regülasyon, marka/IP
- deepseek: Alibaba/tedarik/maliyet bakışı. Canlı Alibaba verisi yoksa bunu açıkça söyle; hangi aramalar, MOQ ve maliyet kalemleri gerektiğini yaz. Maliyet uydurma.

Savaş odası gibi tartış. Ürün fotoğrafı, yorum, video, satıcı sayısı, satış sinyali yoksa kanıt yok de.
Exact satış sayısı uydurma. Reklam yoğunluğu satış değildir.
Çıktı JSON olsun. Kod bloğu kullanma.

Alanlar:
mode, real_ai, summary, score, decision, gpt, gemini, claude, deepseek, common_points, objections, missing_evidence, next_actions, judge, alibaba_research, product_strengths, review_insights, visual_insights

Arrayler 3-5 madde olsun.
decision: GOLD | GÜÇLÜ ADAY | İNCELE | RİSKLİ | PASS

Kullanıcı profili: ${JSON.stringify(payload.profile || {})}
Kanallar: Trendyol, Shopify, Etsy, Alibaba, Yerli üretim, CrossMarket, Meta/Instagram, Amazon
Mesaj: ${payload.message || ""}
Link: ${payload.productUrl || ""}
Metin/Yorum: ${payload.productText || ""}
`.trim();
  try{
    const r = await fetch("https://api.openai.com/v1/responses", {
      method:"POST",
      headers:{"Authorization":`Bearer ${CONFIG.openaiKey}`,"Content-Type":"application/json"},
      body:JSON.stringify({model:CONFIG.openaiModel,input:prompt,store:false,max_output_tokens:3200})
    });
    const data = await r.json().catch(()=>({}));
    if(!r.ok) return {...localCouncil(),mode:"openai_error_fallback",openai_error:data?.error?.message || `OpenAI HTTP ${r.status}`};
    const txt = outputText(data);
    const parsed = parseJsonLoose(txt);
    if(parsed) return normalizeCouncil(parsed,txt);
    const ex = normalizeCouncil({},txt);
    ex.mode = "openai_extracted";
    ex.openai_raw_text = txt.slice(0,900);
    return ex;
  }catch(e){
    return {...localCouncil(),mode:"openai_error_fallback",openai_error:e?.message || "OpenAI bağlantı hatası"};
  }
}

function detectSource(productUrl){
  const u = safeText(productUrl).toLowerCase();
  if (u.includes("shopify")) return "Shopify";
  if (u.includes("trendyol")) return "Trendyol";
  if (u.includes("etsy")) return "Etsy";
  if (u.includes("amazon")) return "Amazon";
  if (u.includes("alibaba")) return "Alibaba";
  return "AI Oda";
}

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

  const council = await openAiCouncil({message,productText,productUrl,profile});
  const source = detectSource(productUrl);

  const result = {
    ok:true,
    id:id("analysis"),
    app:CONFIG.app,
    version:CONFIG.version,
    created_at:now(),
    input:{message,productText,productUrl,profile},
    ai_council:council,
    evidence_gate:{
      locked:Array.isArray(council.missing_evidence) && council.missing_evidence.length > 0,
      reason:"Kritik kanıt eksikse nihai AL kararı açılmaz.",
      missing_evidence:council.missing_evidence || []
    },
    product_images:[],
    product_video:null,
    product_description:council.summary,
    products:[{
      id:id("product"),
      title:message || productText.slice(0,80) || "Ürün adayı",
      source,
      source_provider:council.real_ai ? "openai" : "local_fallback",
      url:productUrl,
      exact_sales_count:null,
      confidence:council.real_ai ? .80 : .45,
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
      ads_policy:"Reklam yoğunluğu satış değildir.",
      evidence_gate:"Kritik kanıt eksikse AL kararı kilitli kalır.",
      live_alibaba:"Canlı Alibaba maliyeti M2 adapter ile bağlanacak."
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
      `SELECT id, created_at, message, product_url, source, score, decision, ai_council, evidence_gate, products
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
