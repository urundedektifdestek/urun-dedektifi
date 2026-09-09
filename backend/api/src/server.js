import http from "node:http";

const HOST = "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

const CONFIG = {
  app: process.env.PUBLIC_API_NAME || "Ürün Dedektifi API",
  version: "1.0.0-m1-ai-room",
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
      if (content.text) chunks.push(content.text);
    }
  }
  return chunks.join("\n").trim();
}

function parseJsonLoose(text){
  const clean = safeText(text).replace(/^```json\s*/i, "").replace(/^```\s*/i, "").replace(/```$/i, "").trim();
  try { return JSON.parse(clean); } catch {}
  const a = clean.indexOf("{"), b = clean.lastIndexOf("}");
  if (a >= 0 && b > a) { try { return JSON.parse(clean.slice(a,b+1)); } catch {} }
  return null;
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
  const decision = score >= 82 ? "GOLD" : score >= 70 ? "GÜÇLÜ ADAY" : score >= 55 ? "İNCELE" : score >= 40 ? "RİSKLİ" : "PASS";
  const missing = ["Canlı Trendyol/Shopify kanıtı","Satıcı yoğunluğu","Tedarikçi fiyatı / MOQ","GTIP / vergi / regülasyon","Marka/IP kontrolü"];
  return {
    mode:"local_fallback",
    real_ai:false,
    score, decision,
    summary:"OPENAI_API_KEY yoksa bu yerel ön analizdir. Uygulama satış sayısı veya maliyet uydurmaz.",
    gpt:"Ticari fırsat için önce talep, satıcı yoğunluğu ve net marj doğrulanmalı.",
    gemini:"Trend sinyali varsa kreatif üretilebilirliği incelenmeli; reklam yoğunluğu satış değildir.",
    claude:"Risk tarafında GTIP, regülasyon, marka/IP ve iade kanıtı eksikse AL kararı kilitli kalmalı.",
    deepseek:"Maliyet tarafında ürün maliyeti, kargo, komisyon, iade ve reklam gideri netleşmeden karar verilmemeli.",
    common_points:positives.length ? positives : ["Kanıt olmadan ürün winner sayılmaz."],
    objections:risks.length ? risks : ["Canlı pazar verisi olmadan nihai karar verilmez."],
    missing_evidence:missing,
    next_actions:["Canlı ürün verisi topla","Tedarik maliyeti doğrula","Evidence Gate’i tamamla"],
    judge:"AI_CHALLENGE: Kritik kanıtlar tamamlanmadan nihai AL kararı kapalıdır.",
    profile_used:payload.profile || {}
  };
}

async function openAiCouncil(payload){
  if (!CONFIG.openaiKey) return localCouncil(payload);
  const prompt = `
Sen Ürün Dedektifi AI Council koordinatörüsün.
Felsefe: "Çok satanı değil, bizim satabileceğimiz çok satanı bul."

Kurallar:
- Exact satış sayısı yoksa asla uydurma.
- Görünür satış/reklam sinyali exact satış değildir.
- Reklam yoğunluğu satış değildir.
- AI, gerçek maliyet/risk/kanıt motorunu geçersiz kılamaz.
- GTIP, vergi, regülasyon, marka/IP, tedarik maliyeti ve satıcı yoğunluğu eksikse AL kararı kilitli kalır.
- Türkiye pazarını esas al.

Kullanıcı profili:
${JSON.stringify(payload.profile || {}, null, 2)}

Mesaj:
${payload.message || ""}

Ürün/trend metni:
${payload.productText || ""}

Ürün linki:
${payload.productUrl || ""}

Sadece JSON döndür:
{"mode":"openai_council","real_ai":true,"summary":"özet","score":0,"decision":"GOLD | GÜÇLÜ ADAY | İNCELE | RİSKLİ | PASS","gpt":"ticari fırsat görüşü","gemini":"trend/pazar görüşü","claude":"risk/kalite itirazı","deepseek":"maliyet/tedarik görüşü","common_points":["ortak fikir"],"objections":["itiraz"],"missing_evidence":["eksik kanıt"],"next_actions":["sıradaki aksiyon"],"judge":"nihai yargı"}
`.trim();

  try {
    const r = await fetch("https://api.openai.com/v1/responses", {
      method:"POST",
      headers:{"Authorization":`Bearer ${CONFIG.openaiKey}`,"Content-Type":"application/json"},
      body:JSON.stringify({model:CONFIG.openaiModel,input:prompt,store:false,max_output_tokens:2200})
    });
    const data = await r.json().catch(() => ({}));
    if (!r.ok) return {...localCouncil(payload), mode:"openai_error_fallback", openai_error:data?.error?.message || `OpenAI HTTP ${r.status}`};
    const txt = outputText(data);
    return parseJsonLoose(txt) || {mode:"openai_text", real_ai:true, summary:txt.slice(0,2200), score:null, decision:"İNCELE", gpt:txt.slice(0,900), gemini:"", claude:"", deepseek:"", common_points:[], objections:[], missing_evidence:["OpenAI JSON dışı yanıt döndürdü."], next_actions:["JSON şeması sıkılaştırılacak."], judge:"Yanıt alındı ama yapılandırma eksik."};
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
  const profile = body.profile || {budget:Number(body.budget || params.get("budget") || 25000), minMargin:Number(body.minMargin || params.get("minMargin") || 0.25), maxSellers:Number(body.maxSellers || params.get("maxSellers") || 7), risk:safeText(body.risk || params.get("risk") || "balanced")};

  if (!message && !productText && !productUrl) return send(res, 400, {ok:false,error:"message, productText veya productUrl gerekli"});

  const council = await openAiCouncil({message, productText, productUrl, profile});
  const result = {
    ok:true,
    id:id("analysis"),
    app:CONFIG.app,
    version:CONFIG.version,
    created_at:now(),
    input:{message, productText, productUrl, profile},
    ai_council:council,
    evidence_gate:{locked:Array.isArray(council.missing_evidence) && council.missing_evidence.length > 0, reason:"Kritik kanıt eksikse nihai AL kararı açılmaz.", missing_evidence:council.missing_evidence || []},
    products:[{id:id("product"), title:message || productText.slice(0,80) || "Ürün adayı", source:"AI Oda", source_provider:council.real_ai ? "openai" : "local_fallback", exact_sales_count:null, confidence:council.real_ai ? 0.72 : 0.45, score:{opportunity_score:council.score, decision:council.decision, reasons:council.common_points || [], risks:council.objections || [], missing_evidence:council.missing_evidence || []}}],
    policy:{exact_sales_count:"Exact satış sayısı yoksa uydurulmaz.", ads_policy:"Reklam yoğunluğu satış değildir.", evidence_gate:"Kritik kanıt eksikse AL kararı kilitli kalır."}
  };
  memory.analyses.unshift(result);
  memory.analyses = memory.analyses.slice(0,50);
  return send(res, 200, result);
}

function status(){
  return {
    ok:true, app:CONFIG.app, version:CONFIG.version, time:now(), uptime_seconds:Math.round(process.uptime()),
    endpoints:{status:"GET /", ai_room:"GET/POST /ai-room", scan:"GET/POST /scan", mobile_compat:"POST /", decision:"POST /decision"},
    env:{openai:!!CONFIG.openaiKey, openai_model:CONFIG.openaiModel, api_token_required:!!CONFIG.apiToken, database_next:!!CONFIG.databaseUrl, serpapi_next:!!CONFIG.serpapiKey, apify_next:!!CONFIG.apifyToken},
    memory_counts:{analyses:memory.analyses.length, decisions:memory.decisions.length}
  };
}

const server = http.createServer(async (req,res) => {
  try {
    const url = new URL(req.url || "/", `http://${req.headers.host || "localhost"}`);
    if (req.method === "OPTIONS") return send(res, 204, {ok:true});
    if (url.pathname === "/favicon.ico") { res.writeHead(204); return res.end(); }
    if (req.method === "GET" && ["/","/health","/ready"].includes(url.pathname)) return send(res, 200, status());

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
