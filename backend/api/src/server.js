import http from "node:http";

const HOST = "0.0.0.0";
const PORT = Number(process.env.PORT || 8080);

const CONFIG = {
  app: process.env.PUBLIC_API_NAME || "Ürün Dedektifi API",
  version: "1.1.0-m1-json-fixed",
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
      if (content.type === "output_text" && content.text) chunks.push(content.text);
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
    const candidate = clean.slice(start, end + 1);
    try { return JSON.parse(candidate); } catch {}
  }
  return null;
}

function safeArray(value){
  return Array.isArray(value) ? value.map(x => safeText(x)).filter(Boolean) : [];
}

function normalizeCouncil(council, fallbackPayload){
  const c = council && typeof council === "object" ? council : {};
  const scoreNum = Number(c.score);
  const score = Number.isFinite(scoreNum) ? Math.max(0, Math.min(100, Math.round(scoreNum))) : 50;
  const decision = ["GOLD","GÜÇLÜ ADAY","İNCELE","RİSKLİ","PASS"].includes(c.decision) ? c.decision : (
    score >= 82 ? "GOLD" : score >= 70 ? "GÜÇLÜ ADAY" : score >= 55 ? "İNCELE" : score >= 40 ? "RİSKLİ" : "PASS"
  );

  return {
    mode: c.mode || "openai_council",
    real_ai: c.real_ai === true,
    summary: safeText(c.summary) || "AI Council analizi üretildi.",
    score,
    decision,
    gpt: safeText(c.gpt) || "Ticari fırsat: Talep, rekabet ve marj kanıtları birlikte doğrulanmalı.",
    gemini: safeText(c.gemini) || "Trend/pazar: Reklam veya ilgi sinyali satış sayısı kabul edilmemeli.",
    claude: safeText(c.claude) || "Risk: GTIP, regülasyon, marka/IP ve iade kanıtı eksikse AL kararı kilitli kalmalı.",
    deepseek: safeText(c.deepseek) || "Maliyet/tedarik: Ürün maliyeti, MOQ, kargo, komisyon ve iade maliyeti netleşmeli.",
    common_points: safeArray(c.common_points),
    objections: safeArray(c.objections),
    missing_evidence: safeArray(c.missing_evidence),
    next_actions: safeArray(c.next_actions),
    judge: safeText(c.judge) || "Kritik kanıtlar tamamlanmadan nihai AL kararı kilitli."
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
  };
}

async function openAiCouncil(payload){
  if (!CONFIG.openaiKey) return localCouncil(payload);

  const prompt = `
Sen Ürün Dedektifi AI Council koordinatörüsün.
Felsefe: "Çok satanı değil, bizim satabileceğimiz çok satanı bul."

ÇOK ÖNEMLİ:
Sadece GEÇERLİ JSON döndür.
Markdown, açıklama, kod bloğu, üç tırnak, fazladan metin kullanma.
Alanları kısa tut. Her AI görüşü en fazla 2 cümle olsun.

Kurallar:
- Exact satış sayısı yoksa asla uydurma.
- Görünür satış/reklam sinyali exact satış değildir.
- Reklam yoğunluğu satış değildir.
- AI gerçek maliyet/risk/kanıt motorunu geçersiz kılamaz.
- GTIP, vergi, regülasyon, marka/IP, tedarik maliyeti ve satıcı yoğunluğu eksikse AL kararı kilitli kalır.
- Türkiye pazarını esas al.

Kullanıcı profili:
${JSON.stringify(payload.profile || {})}

Mesaj:
${payload.message || ""}

Ürün/trend metni:
${payload.productText || ""}

Ürün linki:
${payload.productUrl || ""}

JSON ŞEMASI:
{
  "mode": "openai_council",
  "real_ai": true,
  "summary": "kısa özet",
  "score": 0,
  "decision": "GOLD | GÜÇLÜ ADAY | İNCELE | RİSKLİ | PASS",
  "gpt": "ticari fırsat görüşü",
  "gemini": "trend/pazar görüşü",
  "claude": "risk/kalite itirazı",
  "deepseek": "maliyet/tedarik görüşü",
  "common_points": ["ortak fikir 1", "ortak fikir 2"],
  "objections": ["itiraz 1", "itiraz 2"],
  "missing_evidence": ["eksik kanıt 1", "eksik kanıt 2"],
  "next_actions": ["aksiyon 1", "aksiyon 2"],
  "judge": "nihai yargı"
}
`.trim();

  try {
    const requestBody = {
      model: CONFIG.openaiModel,
      input: prompt,
      store: false,
      max_output_tokens: 6000
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

    if (parsed) {
      return normalizeCouncil(parsed, payload);
    }

    return {
      ...localCouncil(payload),
      mode:"openai_parse_error_fallback",
      real_ai:true,
      openai_raw_text: txt.slice(0, 1200),
      summary:"OpenAI cevap verdi ancak JSON ayrıştırılamadı. Yerel güvenli karar motoru devreye girdi.",
      missing_evidence:["OpenAI JSON parse hatası", "Canlı pazar kanıtı", "Tedarik/GTIP/regülasyon kanıtı"]
    };
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
      confidence:council.real_ai ? 0.72 : 0.45,
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
