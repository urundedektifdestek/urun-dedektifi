import http from "node:http";

const HOST = "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

const CONFIG = {
  app: process.env.PUBLIC_API_NAME || "Ürün Dedektifi API",
  version: "1.3.0-m1-compact-ai",
  openaiKey: process.env.OPENAI_API_KEY || "",
  openaiModel: process.env.OPENAI_MODEL || "gpt-5-mini",
  apiToken: process.env.API_TOKEN || "",
  databaseUrl: process.env.DATABASE_URL || "",
  serpapiKey: process.env.SERPAPI_KEY || "",
  apifyToken: process.env.APIFY_TOKEN || ""
};

const memory = { analyses: [], decisions: [], created_at: new Date().toISOString() };

function now(){ return new Date().toISOString(); }
function id(prefix){ return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2,8)}`; }
function safeText(v){ return v === null || v === undefined ? "" : String(v).trim(); }

function send(res, status, data){
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,X-API-Token",
    "Cache-Control": "no-store"
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
    for (const content of item.content || []) {
      if (typeof content.text === "string") chunks.push(content.text);
      if (content.type === "output_text" && typeof content.text === "string") chunks.push(content.text);
    }
  }
  return chunks.join("\n").trim();
}

function parseJsonLoose(text){
  const clean = safeText(text)
    .replace(/^```json\s*/i, "")
    .replace(/^```\s*/i, "")
    .replace(/```$/i, "")
    .trim();

  try { return JSON.parse(clean); } catch {}

  const start = clean.indexOf("{");
  const end = clean.lastIndexOf("}");
  if (start >= 0 && end > start) {
    try { return JSON.parse(clean.slice(start, end + 1)); } catch {}
  }
  return null;
}

function extractString(src, key){
  const re = new RegExp(`"${key}"\\s*:\\s*"((?:\\\\.|[^"\\\\])*)"`, "s");
  const m = safeText(src).match(re);
  if (!m) return "";
  try { return JSON.parse(`"${m[1]}"`); } catch { return m[1]; }
}

function extractNumber(src, key){
  const re = new RegExp(`"${key}"\\s*:\\s*(\\d+)`);
  const m = safeText(src).match(re);
  return m ? Number(m[1]) : null;
}

function extractArray(src, key){
  const re = new RegExp(`"${key}"\\s*:\\s*\\[([\\s\\S]*?)\\]`, "s");
  const m = safeText(src).match(re);
  if (!m) return [];
  try { return JSON.parse(`[${m[1]}]`).map(x => safeText(x)).filter(Boolean); } catch {}
  const values = [];
  const itemRe = /"((?:\\.|[^"\\])*)"/g;
  let im;
  while ((im = itemRe.exec(m[1])) !== null) {
    try { values.push(JSON.parse(`"${im[1]}"`)); } catch { values.push(im[1]); }
  }
  return values.filter(Boolean);
}

function safeArray(value){
  return Array.isArray(value) ? value.map(x => safeText(x)).filter(Boolean) : [];
}

function decisionFromScore(score){
  return score >= 82 ? "GOLD" : score >= 70 ? "GÜÇLÜ ADAY" : score >= 55 ? "İNCELE" : score >= 40 ? "RİSKLİ" : "PASS";
}

function normalizeCouncil(raw, sourceText = ""){
  const c = raw && typeof raw === "object" ? raw : {};
  const extractedScore = extractNumber(sourceText, "score");
  const scoreNum = Number(c.score ?? extractedScore);
  const score = Number.isFinite(scoreNum) ? Math.max(0, Math.min(100, Math.round(scoreNum))) : 50;

  const extractedDecision = extractString(sourceText, "decision");
  const allowed = ["GOLD","GÜÇLÜ ADAY","İNCELE","RİSKLİ","PASS"];
  const decision = allowed.includes(c.decision) ? c.decision : allowed.includes(extractedDecision) ? extractedDecision : decisionFromScore(score);

  return {
    mode: "openai_council",
    real_ai: true,
    summary: safeText(c.summary) || extractString(sourceText, "summary") || "AI Council analizi üretildi.",
    score,
    decision,
    gpt: safeText(c.gpt) || extractString(sourceText, "gpt") || "Talep, rekabet ve marj kanıtları doğrulanmalı.",
    gemini: safeText(c.gemini) || extractString(sourceText, "gemini") || "Trend sinyali satış sayısı kabul edilmemeli.",
    claude: safeText(c.claude) || extractString(sourceText, "claude") || "Kritik kanıt eksikse AL kararı kilitli kalmalı.",
    deepseek: safeText(c.deepseek) || extractString(sourceText, "deepseek") || "Tedarik ve maliyet netleşmeli.",
    common_points: safeArray(c.common_points).length ? safeArray(c.common_points) : extractArray(sourceText, "common_points"),
    objections: safeArray(c.objections).length ? safeArray(c.objections) : extractArray(sourceText, "objections"),
    missing_evidence: safeArray(c.missing_evidence).length ? safeArray(c.missing_evidence) : extractArray(sourceText, "missing_evidence"),
    next_actions: safeArray(c.next_actions).length ? safeArray(c.next_actions) : extractArray(sourceText, "next_actions"),
    judge: safeText(c.judge) || extractString(sourceText, "judge") || "Kritik kanıtlar tamamlanmadan nihai AL kararı kilitli."
  };
}

function localCouncil(payload){
  const combined = `${payload.message} ${payload.productText} ${payload.productUrl}`.toLowerCase();
  let score = 52;
  const positives = [], risks = [];
  if (combined.includes("trend") || combined.includes("viral") || combined.includes("reklam")) {
    score += 12; positives.push("Trend/reklam ilgisi var; ama bu satış sayısı değildir.");
  }
  if (combined.includes("alibaba") || combined.includes("yerli") || combined.includes("üret")) {
    score += 8; positives.push("Tedarik avantajı araştırılabilir.");
  }
  if (combined.includes("kozmetik") || combined.includes("gıda") || combined.includes("bebek") || combined.includes("elektronik")) {
    score -= 10; risks.push("Kategori regülasyon/iade açısından hassas olabilir.");
  }
  if (combined.includes("çok satıcı") || combined.includes("rekabet")) {
    score -= 8; risks.push("Satıcı yoğunluğu kontrol edilmeli.");
  }
  score = Math.max(0, Math.min(100, Math.round(score)));
  const missing = ["Canlı Trendyol/Shopify kanıtı","Satıcı yoğunluğu","Tedarikçi fiyatı / MOQ","GTIP / vergi / regülasyon","Marka/IP kontrolü"];
  return {
    mode:"local_fallback",
    real_ai:false,
    score,
    decision: decisionFromScore(score),
    summary:"OPENAI_API_KEY yoksa bu yerel ön analizdir. Uygulama satış sayısı veya maliyet uydurmaz.",
    gpt:"Ticari fırsat için önce talep, satıcı yoğunluğu ve net marj doğrulanmalı.",
    gemini:"Trend sinyali varsa kreatif üretilebilirliği incelenmeli; reklam yoğunluğu satış değildir.",
    claude:"Risk tarafında GTIP, regülasyon, marka/IP ve iade kanıtı eksikse AL kararı kilitli kalmalı.",
    deepseek:"Maliyet tarafında ürün maliyeti, kargo, komisyon, iade ve reklam gideri netleşmeden karar verilmemeli.",
    common_points:positives.length ? positives : ["Kanıt olmadan ürün winner sayılmaz."],
    objections:risks.length ? risks : ["Canlı pazar verisi olmadan nihai karar verilmez."],
    missing_evidence:missing,
    next_actions:["Canlı ürün verisi topla","Tedarik maliyeti doğrula","Evidence Gate’i tamamla"],
    judge:"AI_CHALLENGE: Kritik kanıtlar tamamlanmadan nihai AL kararı kapalıdır."
  };
}

async function openAiCouncil(payload){
  if (!CONFIG.openaiKey) return localCouncil(payload);

  const prompt = `
Sen Ürün Dedektifi AI Council koordinatörüsün.
Türkiye pazarı için ürün fırsatı analizi yap.

MUTLAK KURAL:
Sadece tek satırlık kısa JSON döndür. Markdown yok. Kod bloğu yok.
Her metin alanı en fazla 120 karakter olsun.
Array alanları en fazla 3 madde olsun.

Alanlar:
mode, real_ai, summary, score, decision, gpt, gemini, claude, deepseek, common_points, objections, missing_evidence, next_actions, judge

Sabitler:
mode="openai_council"
real_ai=true
decision sadece şunlardan biri: GOLD, GÜÇLÜ ADAY, İNCELE, RİSKLİ, PASS

İlkeler:
Exact satış sayısı uydurma.
Reklam yoğunluğu satış değildir.
GTIP/vergi/regülasyon/marka/tedarik eksikse AL kararı kilitlidir.

Kullanıcı profili: ${JSON.stringify(payload.profile || {})}
Mesaj: ${payload.message || ""}
Ürün/metin: ${payload.productText || ""}
Link: ${payload.productUrl || ""}

JSON örneği:
{"mode":"openai_council","real_ai":true,"summary":"Kısa özet","score":55,"decision":"İNCELE","gpt":"Ticari görüş","gemini":"Trend görüşü","claude":"Risk itirazı","deepseek":"Maliyet görüşü","common_points":["Kanıt gerekli"],"objections":["Rekabet bilinmiyor"],"missing_evidence":["Tedarik","GTIP"],"next_actions":["Pazar tara"],"judge":"İncele, AL kilitli"}
`.trim();

  try {
    const requestBody = {
      model: CONFIG.openaiModel,
      input: prompt,
      store: false,
      max_output_tokens: 1400
    };

    const r = await fetch("https://api.openai.com/v1/responses", {
      method:"POST",
      headers:{
        "Authorization":`Bearer ${CONFIG.openaiKey}`,
        "Content-Type":"application/json"
      },
      body:JSON.stringify(requestBody)
    });

    const data = await r.json().catch(() => ({}));

    if (!r.ok) {
      return {
        ...localCouncil(payload),
        mode:"openai_error_fallback",
        openai_error:data?.error?.message || `OpenAI HTTP ${r.status}`
      };
    }

    const txt = outputText(data);
    const parsed = parseJsonLoose(txt);

    if (parsed) return normalizeCouncil(parsed, txt);

    const extracted = normalizeCouncil({}, txt);
    extracted.mode = "openai_extracted";
    extracted.openai_raw_text = txt.slice(0, 900);
    return extracted;
  } catch(e) {
    return {...localCouncil(payload), mode:"openai_error_fallback", openai_error:e?.message || "OpenAI bağlantı hatası"};
  }
}

async function analyze(req, res, body, params){
  const deny = checkAuth(req);
  if (deny) return send(res, 401, deny);

  const message = safeText(body.message || body.question || body.query || params.get("message") || params.get("q"));
  const productText = safeText(body.productText || body.pasted || body.text || params.get("productText") || params.get("text"));
  const productUrl = safeText(body.productUrl || body.url || params.get("url"));
  const profile = body.profile || {
    budget:Number(body.budget || params.get("budget") || 25000),
    minMargin:Number(body.minMargin || params.get("minMargin") || 0.25),
    maxSellers:Number(body.maxSellers || params.get("maxSellers") || 7),
    risk:safeText(body.risk || params.get("risk") || "balanced")
  };

  if (!message && !productText && !productUrl) {
    return send(res, 400, {ok:false,error:"message, productText veya productUrl gerekli"});
  }

  const council = await openAiCouncil({message, productText, productUrl, profile});
  const result = {
    ok:true,
    id:id("analysis"),
    app:CONFIG.app,
    version:CONFIG.version,
    created_at:now(),
    input:{message, productText, productUrl, profile},
    ai_council:council,
    evidence_gate:{
      locked:Array.isArray(council.missing_evidence) && council.missing_evidence.length > 0,
      reason:"Kritik kanıt eksikse nihai AL kararı açılmaz.",
      missing_evidence:council.missing_evidence || []
    },
    products:[{
      id:id("product"),
      title:message || productText.slice(0,80) || "Ürün adayı",
      source:"AI Oda",
      source_provider:council.real_ai ? "openai" : "local_fallback",
      exact_sales_count:null,
      confidence:council.real_ai ? 0.80 : 0.45,
      score:{
        opportunity_score:council.score,
        decision:council.decision,
        reasons:council.common_points || [],
        risks:council.objections || [],
        missing_evidence:council.missing_evidence || []
      }
    }],
    policy:{
      exact_sales_count:"Exact satış sayısı yoksa uydurulmaz.",
      ads_policy:"Reklam yoğunluğu satış değildir.",
      evidence_gate:"Kritik kanıt eksikse AL kararı kilitli kalır."
    }
  };

  memory.analyses.unshift(result);
  memory.analyses = memory.analyses.slice(0,50);
  return send(res, 200, result);
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
      mobile_compat:"POST /",
      decision:"POST /decision"
    },
    env:{
      openai:!!CONFIG.openaiKey,
      openai_model:CONFIG.openaiModel,
      compact_ai:true,
      api_token_required:!!CONFIG.apiToken,
      database_next:!!CONFIG.databaseUrl,
      serpapi_next:!!CONFIG.serpapiKey,
      apify_next:!!CONFIG.apifyToken
    },
    memory_counts:{analyses:memory.analyses.length, decisions:memory.decisions.length}
  };
}

const server = http.createServer(async (req,res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);

    if (req.method === "OPTIONS") return send(res, 204, {ok:true});
    if (url.pathname === "/favicon.ico") { res.writeHead(204); return res.end(); }

    if (req.method === "GET" && ["/","/health","/ready"].includes(url.pathname)) {
      return send(res, 200, status());
    }

    if (["/ai-room","/scan","/api/scan/new"].includes(url.pathname)) {
      const body = req.method === "POST" ? await readBody(req) : {};
      return analyze(req, res, body, url.searchParams);
    }

    if (url.pathname === "/" && req.method === "POST") {
      const body = await readBody(req);
      return analyze(req, res, body, url.searchParams);
    }

    if (url.pathname === "/decision" && req.method === "POST") {
      const deny = checkAuth(req);
      if (deny) return send(res, 401, deny);
      const body = await readBody(req);
      const item = {id:id("decision"), created_at:now(), ...body};
      memory.decisions.unshift(item);
      return send(res, 200, {ok:true, decision:item});
    }

    if (url.pathname === "/memory" && req.method === "GET") return send(res, 200, {ok:true, memory});

    return send(res, 404, {ok:false, error:"endpoint_not_found", path:url.pathname});
  } catch(e) {
    console.error("server_error", e);
    return send(res, 500, {ok:false, error:"server_error", message:e?.message || "Bilinmeyen hata"});
  }
});

server.listen(PORT, HOST, () => console.log(`Ürün Dedektifi API ${CONFIG.version} running on http://${HOST}:${PORT}`));
