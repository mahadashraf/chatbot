// server.js
import "dotenv/config";
import { load } from "cheerio";
import express from "express";
import cors from "cors";
import OpenAI from "openai";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { fetch as undiciFetch } from "undici";

// Polyfill fetch for Node < 18
if (typeof globalThis.fetch !== "function") {
  globalThis.fetch = undiciFetch;
}

/* ====================== CONFIG ====================== */
const STORE_DOMAIN   = process.env.STORE_DOMAIN;          // yourshop.myshopify.com (no protocol)
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const PORT           = process.env.PORT || 3000;
const MODEL_NAME     = process.env.OPENAI_MODEL || "gpt-4o-mini";

// Log directory setup (JSONL)
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const LOG_DIR = process.env.LOG_DIR || path.join(__dirname, "logs");
const LOG_FILE = path.join(LOG_DIR, "chat_responses.jsonl");
try { if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true }); } catch {}
function logChatResponse(entry) {
  try { fs.appendFile(LOG_FILE, JSON.stringify(entry) + "\n", () => {}); } catch {}
}

// Bulk ingest knobs (safe defaults; tune via .env)
const INGEST_CONCURRENCY    = Number(process.env.INGEST_CONCURRENCY || 50);   // parallel fetches
const INGEST_TIMEOUT_MS     = Number(process.env.INGEST_TIMEOUT_MS  || 20000); // per-task timeout
const INGEST_RETRIES        = Number(process.env.INGEST_RETRIES     || 2);     // retries per handle
const INGEST_BATCH_DELAY_MS = Number(process.env.INGEST_BATCH_DELAY_MS || 200);// soft pacing between launches

if (!STORE_DOMAIN || !OPENAI_API_KEY) {
  console.error("Missing STORE_DOMAIN or OPENAI_API_KEY in .env");
  process.exit(1);
}

/* ====================== APP ====================== */
const app = express();
// Respect proxy headers for correct IP/session keys when behind CDNs/load balancers
app.set("trust proxy", true);

// CORS setup: allow specific origins from env, default to permissive for development
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || "*")
  .split(",")
  .map(s => s.trim())
  .filter(Boolean);
const corsOptions = {
  origin: (origin, cb) => {
    if (!origin || origin === "null") return cb(null, true);
    if (ALLOWED_ORIGINS.includes("*")) return cb(null, true);
    const ok = ALLOWED_ORIGINS.some(o => origin === o);
    return cb(ok ? null : new Error("CORS blocked"), ok);
  },
  methods: ["GET","POST","OPTIONS"],
  allowedHeaders: ["Content-Type"],
  credentials: true,
  maxAge: 86400,
};
app.use(cors(corsOptions));
app.options("*", cors(corsOptions));
app.use(express.json());
const openai = new OpenAI({ apiKey: OPENAI_API_KEY });

// OpenAI fallback model helper
const FALLBACK_MODELS = (process.env.OPENAI_FALLBACK_MODELS || "gpt-4o,gpt-4o-mini-2024-07-18").split(",").map(s => s.trim()).filter(Boolean);
function getModelChain(primary = MODEL_NAME) {
  const seen = new Set();
  const chain = [];
  const add = (m) => { if (m && !seen.has(m)) { seen.add(m); chain.push(m); } };
  add(primary);
  for (const m of FALLBACK_MODELS) add(m);
  return chain;
}
async function chatWithFallback(messages = [], params = {}) {
  let lastErr = null;
  for (const model of getModelChain(params.model || MODEL_NAME)) {
    try {
      const r = await openai.chat.completions.create({
        model,
        messages,
        temperature: 0.2,
        ...params,
        // Defensive max tokens
        max_tokens: Math.min(800, params.max_tokens || 800),
      });
      return { response: r, model };
    } catch (e) {
      lastErr = e;
      if (e?.status === 429 || (e?.status >= 500 && e?.status <= 599)) {
        continue; // try next model
      }
      throw e;
    }
  }
  throw lastErr || new Error("All models failed");
}


// Quick env + network health
app.get("/health", async (_req, res) => {
  const out = {
    node: process.version,
    store_domain: process.env.STORE_DOMAIN,
    has_openai_key: !!process.env.OPENAI_API_KEY,
    port: PORT,
    can_fetch_store: null,
    sample_handle_ok: null,
  };
  try {
    // Try public products.json first page
    const r = await fetch(`https://${STORE_DOMAIN}/products.json?limit=1`);
    out.can_fetch_store = r.ok ? "ok" : `http ${r.status}`;
    if (r.ok) {
      const j = await r.json();
      const h = j?.products?.[0]?.handle;
      if (h) {
        const p = await fetch(`https://${STORE_DOMAIN}/products/${h}.js`);
        out.sample_handle_ok = p.ok ? "ok" : `http ${p.status}`;
      }
    }
  } catch (e) {
    out.can_fetch_store = String(e?.message || e);
  }
  res.json(out);
});

// OpenAI key sanity check (no charge, 1 small request)
app.get("/test-openai", async (_req, res) => {
  try {
    const { response, model } = await chatWithFallback([
      { role: "user", content: "ping" }
    ], { max_tokens: 1, temperature: 0 });
    res.json({ ok: true, model, id: response.id });
  } catch (e) {
    res.status(500).json({ ok: false, error: e?.error?.message || e.message });
  }
});


/* ============ SIMPLE IN-MEMORY CONVERSATION STATE ============ */
let LAST_PRODUCT = null;

/* ============ QUIZ STATE (IN-MEMORY) ============ */
const QUIZ_SESSIONS = new Map(); // key -> { stepIndex, answers, createdAt }
const QUIZ_TTL_MS = 15 * 60 * 1000; // 15 minutes

/* ============ USER IDENTITY (IN-MEMORY) ============ */
const USER_SESSIONS = new Map(); // key -> { name, email, createdAt }
const USER_TTL_MS = 24 * 60 * 60 * 1000; // 24 hours

const QUIZ_QUESTIONS = [
  {
    id: "location",
    prompt: "Do you prefer an indoor or outdoor sauna?",
    options: [
      { code: "A", label: "Indoor", value: "indoor" },
      { code: "B", label: "Outdoor", value: "outdoor" },
    ],
  },
  {
    id: "reason",
    prompt: "What’s your main reason for getting a sauna?",
    options: [
      { code: "A", label: "Relaxation", value: "relaxation" },
      { code: "B", label: "Detox", value: "detox" },
      { code: "C", label: "Muscle recovery", value: "recovery" },
      { code: "D", label: "Social time", value: "social" },
      { code: "E", label: "Other", value: "other" },
    ],
  },
  {
    id: "heater_type",
    prompt: "What type of heater do you prefer?",
    options: [
      { code: "A", label: "Electric", value: "electric" },
      { code: "B", label: "Wood-burning", value: "wood" },
      { code: "C", label: "Either is fine", value: "any" },
    ],
  },
  {
    id: "heat",
    prompt: "Which style do you like most?",
    options: [
      { code: "A", label: "Traditional Finnish", value: "traditional" },
      { code: "B", label: "Infrared", value: "infrared" },
      { code: "C", label: "Hybrid", value: "hybrid" },
      { code: "D", label: "Not sure yet", value: "any" },
    ],
  },
  {
    id: "capacity",
    prompt: "How many people should it fit at once?",
    options: [
      { code: "A", label: "1", value: "1" },
      { code: "B", label: "2", value: "2" },
      { code: "C", label: "3–4", value: "3-4" },
      { code: "D", label: "5+", value: "5+" },
    ],
  },
  {
    id: "power",
    prompt: "Do you know your available power supply?",
    options: [
      { code: "A", label: "120V", value: "120v" },
      { code: "B", label: "240V", value: "240v" },
      { code: "C", label: "Not sure", value: "unsure" },
    ],
  },
  {
    id: "budget",
    prompt: "What’s your estimated budget range?",
    options: [
      { code: "A", label: "Under $2,000", value: "<2000" },
      { code: "B", label: "$2,000–$5,000", value: "2000-5000" },
      { code: "C", label: "$5,000–$10,000", value: "5000-10000" },
      { code: "D", label: "Over $10,000", value: ">10000" },
      { code: "E", label: "Still exploring", value: "exploring" },
    ],
  },
  {
    id: "timeline",
    prompt: "When do you plan to purchase?",
    options: [
      { code: "A", label: "ASAP", value: "asap" },
      { code: "B", label: "This month", value: "month" },
      { code: "C", label: "1–3 months", value: "1-3" },
      { code: "D", label: "Just researching", value: "researching" },
    ],
  },
  {
    id: "extras",
    prompt: "Any must-have extras? (Steam, chromotherapy lighting, cold plunge, Wi‑Fi, etc.)",
    options: [
      { code: "A", label: "Steam", value: "steam" },
      { code: "B", label: "Chromotherapy lighting", value: "chromotherapy" },
      { code: "C", label: "Cold plunge", value: "cold_plunge" },
      { code: "D", label: "Wi‑Fi/Smart control", value: "wifi" },
      { code: "E", label: "Other/None", value: "other" },
    ],
  },
];
const QUIZ_COUNT = QUIZ_QUESTIONS.length;

function getSessionKey(req) {
  const ip = req.headers["x-forwarded-for"]?.split(",")[0]?.trim() || req.ip || "unknown";
  const ua = (req.headers["user-agent"] || "").slice(0, 40);
  return `${ip}|${ua}`;
}
function cleanupQuizSessions() {
  const now = Date.now();
  for (const [k, v] of QUIZ_SESSIONS.entries()) {
    if (!v?.createdAt || now - v.createdAt > QUIZ_TTL_MS) QUIZ_SESSIONS.delete(k);
  }
}
function formatQuizQuestion(stepIndex) {
  const q = QUIZ_QUESTIONS[stepIndex];
  return `${stepIndex + 1}. ${q.prompt}`;
}
function startQuizSession(sessionKey) {
  const state = { stepIndex: 0, answers: {}, createdAt: Date.now() };
  QUIZ_SESSIONS.set(sessionKey, state);
  return state;
}
function parseQuizAnswer(stepIndex, text) {
  const q = QUIZ_QUESTIONS[stepIndex];
  const raw = (text || "").toString();
  const s = raw.trim().toUpperCase();
  const sl = raw.trim().toLowerCase();
  // Accept letter codes OR full label keywords
  const m = s.match(/\b([A-E])\b/);
  if (m) {
    const opt = q.options.find((o) => o.code === m[1]);
    if (opt) return opt.value;
  }
  // Match by label words or value keywords
  for (const opt of q.options) {
    const label = (opt.label || "").toLowerCase();
    const words = label.split(/[^a-z0-9]+/).filter(Boolean);
    if (words.some(w => sl.includes(w))) return opt.value;
    if (sl.includes(String(opt.value))) return opt.value;
  }
  return null;
}
function describeAnswers(ans) {
  const parts = [];
  for (const q of QUIZ_QUESTIONS) {
    const v = ans[q.id];
    if (v == null || v === "") continue;
    const opt = (q.options || []).find((o) => o.value === v);
    parts.push(`${q.id}: ${opt ? opt.label : v}`);
  }
  return parts.join("; ");
}

// Prefill quiz answers from free text (e.g., "which indoor sauna ...")
function prefillQuizAnswersFromText(text = "") {
  const s = String(text).toLowerCase();
  const ans = {};
  if (/\bindoor\b/.test(s)) ans.location = "indoor";
  else if (/\boutdoor\b/.test(s)) ans.location = "outdoor";

  if (/\binfrared\b/.test(s)) ans.heat = "infrared";
  else if (/\bhybrid\b/.test(s)) ans.heat = "hybrid";
  else if (/(traditional|electric|wood|steam)/.test(s)) ans.heat = "traditional";

  if (/\bbarrel\b/.test(s)) ans.style = "barrel";
  else if (/(cabin|rectangular|rect|cube)/.test(s)) ans.style = "rect";

  const capNum = s.match(/\b(1|2|3|4|5|6|7|8)\s*(person|people|seater)?\b/);
  if (capNum) {
    const n = parseInt(capNum[1], 10);
    if (n <= 2) ans.capacity = String(n);
    else if (n === 3 || n === 4) ans.capacity = "3-4";
    else if (n >= 5) ans.capacity = "5+";
  }
  if (/(3\s*[-–to]\s*4)/.test(s)) ans.capacity = "3-4";
  if (/(5\s*\+|5\s*\+\s*people|5\s*or\s*more)/.test(s)) ans.capacity = "5+";

  const bUnder = s.match(/under\s*\$?\s*(\d{3,5})/);
  const bBetween = s.match(/\$?\s*(\d{3,5})\s*(?:-|to|–)\s*\$?\s*(\d{3,5})/);
  const bAbove = s.match(/over\s*\$?\s*(\d{3,5})/);
  if (bUnder) {
    const v = Number(bUnder[1]);
    if (v <= 2000) ans.budget = "<2000";
    else if (v <= 3000) ans.budget = "<3000";
  } else if (bBetween) {
    const a = Number(bBetween[1]);
    const b = Number(bBetween[2]);
    if (a <= 2000 && b <= 5000) ans.budget = "2000-5000";
    else if (a <= 3000 && b <= 6000) ans.budget = "3000-6000";
    else if (a <= 6000 && b <= 10000) ans.budget = "6000-10000";
  } else if (bAbove) {
    const v = Number(bAbove[1]);
    if (v >= 10000) ans.budget = ">10000";
  }
  return ans;
}

// Determine quiz domain (sauna vs heater)
function detectQuizDomainFromText(text = "") {
  const s = String(text).toLowerCase();
  return /\bheater\b/.test(s) ? "heater" : "sauna";
}

function shouldAskCustomFirstFromText(domain, text = "") {
  const s = String(text).toLowerCase();
  if (domain === "heater") {
    // If user already specified heater type, skip
    return !(/\b(electric|wood(-|\s*)burn|woodburn)\b/.test(s));
  } else {
    // If user already specified sauna subtype/location, skip
    return !(/\b(infrared|traditional|outdoor|barrel|cube)\b/.test(s));
  }
}

function formatCustomFirstQuestion(domain) {
  if (domain === "heater") {
    return [
      "1. Which heater type do you prefer?",
      "Tap an option below.",
    ].join("\n");
  }
  return [
    "1. Which sauna type are you most interested in?",
    "Tap an option below.",
  ].join("\n");
}

function parseCustomFirstAnswer(domain, text) {
  const raw = String(text || "");
  const s = raw.trim().toUpperCase();
  const sl = raw.trim().toLowerCase();
  const result = {};
  if (domain === "heater") {
    // Accept A/B/C or keyword text
    if (/\bA\b/.test(s) || /\belectric\b/i.test(sl)) result.heater_type = "electric";
    else if (/\bB\b/.test(s) || /(wood|wood[-\s]*burn)/i.test(sl)) result.heater_type = "wood";
    else if (/\bC\b/.test(s) || /no\s*preference|either|any/i.test(sl)) result.heater_type = "any";
    return result;
  }
  // Sauna domain: accept letters or full labels
  if (/\bA\b/.test(s) || /traditional|electric|wood/i.test(sl)) result.heat = "traditional";
  else if (/\bB\b/.test(s) || /infrared/i.test(sl)) result.heat = "infrared";
  else if (/\bC\b/.test(s) || /outdoor/i.test(sl)) result.location = "outdoor";
  else if (/\bD\b/.test(s) || /indoor/i.test(sl)) result.location = "indoor";
  else if (/\bE\b/.test(s) || /barrel/i.test(sl)) result.style = "barrel";
  else if (/\bF\b/.test(s) || /no\s*preference|either|any/i.test(sl)) {}
  return result;
}

// Build quick-reply options for the UI (handled by index.html)
function quickRepliesForStep(stepIndex) {
  const q = QUIZ_QUESTIONS[stepIndex];
  if (!q) return [];
  // Provide label-only to allow clients that do not display letter codes
  return (q.options || []).map(opt => ({ code: opt.code, label: `${opt.label}` }));
}
function quickRepliesForCustom(domain) {
  if (domain === "heater") {
    return [
      { code: "A", label: "Electric" },
      { code: "B", label: "Wood-burning" },
      { code: "C", label: "No preference" },
    ];
  }
  return [
    { code: "A", label: "Traditional" },
    { code: "B", label: "Infrared" },
    { code: "C", label: "Outdoor" },
    { code: "D", label: "Indoor" },
    { code: "E", label: "Barrel" },
    { code: "F", label: "No preference" },
  ];
}

// Greeting quick replies
function quickRepliesForGreeting() {
  return [
    { code: "QUIZ", label: " Find My Perfect Sauna " },
    { code: "HEATER", label: " Sauna Heater & Stones Advice" },
    { code: "SHIPPING", label: " Delivery & Lead Time Info" },
    { code: "CONTACT", label: " Contact Support" },
  ];
}

function parseGreetingAction(text = ""){
  const s = String(text).toLowerCase();
  if (/find my perfect sauna|quiz\b|^\s*quiz\s*$/i.test(text)) return "QUIZ";
  if (/(heater|stones? advice|stones)/.test(s)) return "HEATER";
  if (/(delivery|lead\s*time|shipping)/.test(s)) return "SHIPPING";
  if (/(contact|support|help desk|customer service)/.test(s)) return "CONTACT";
  return null;
}

function inferProductAttributes(product) {
  const hay = (product._hay_all || "") + " " + (product.title || "");
  const title = (product.title || "").toLowerCase();
  // Heat detection with support for hybrid
  let heat = null;
  if (/hybrid/i.test(hay)) heat = "hybrid";
  else if (/infrared/i.test(hay)) heat = "infrared";
  else if (/(traditional|electric\s*heater|wood\b|steam)/i.test(hay)) heat = "traditional";

  // Heater type (electric vs wood-burning) — best-effort
  let heaterType = null;
  if (/(wood[-\s]*(burn(ing)?|stove|fired))/i.test(hay)) heaterType = "wood";
  else if (/(electric\s*(heater|stove)|\b240v\b|\b120v\b)/i.test(hay)) heaterType = "electric";

  // Power detection (approximate)
  let power = null;
  if (/(\b110v\b|\b115v\b|\b120v\b)/i.test(hay)) power = "120v";
  else if (/(\b220v\b|\b230v\b|\b240v\b)/i.test(hay)) power = "240v";

  const attrs = {
    location: /outdoor|barrel/i.test(hay) ? "outdoor" : (/indoor/i.test(hay) ? "indoor" : null),
    heat,
    style: /barrel/i.test(hay) ? "barrel" : "rect",
    capacity: null,
    price: Number.isFinite(product.price_from) ? product.price_from : null,
    heater_type: heaterType,
    power,
  };
  const capm = hay.match(/\b(1|2|3|4|5|6|7|8)\s*[- ]?\s*(person|people|seater)\b/i);
  if (capm) attrs.capacity = parseInt(capm[1], 10);
  return attrs;
}

// Heuristic: keep full sauna units; filter out accessories and standalone heaters
function isLikelySaunaProduct(product) {
  const title = String(product?.title || "").toLowerCase();
  const hayAll = String(product?._hay_all || "").toLowerCase();
  // Must contain "sauna" or "saunas" in title or body text (avoid generic accessories without context)
  const containsSauna = /\bsaunas?\b/i.test(title) || /\bsaunas?\b/i.test(hayAll);
  if (!containsSauna) return false;
  // Exclude common accessory terms
  const accessoryRx = /(sconce|light|lighting|kit|floor kit|stones|thermometer|hygrometer|bucket|ladle|backrest|headrest|pillow|cushion|towel|oil|aroma|essence|lamp|controller|control panel|cable|mat|underlay|cover|salt|timer|sand\s*timer)/i;
  if (accessoryRx.test(title)) return false;
  // Exclude heaters when looking specifically for sauna units
  if (/\bheater\b/.test(title)) return false;
  return true;
}
function scoreProductForQuiz(product, ans) {
  const attrs = inferProductAttributes(product);
  let score = 0;
  // Location
  if (ans.location && attrs.location) {
    if (ans.location === attrs.location) score += 20; else score -= 10;
  }
  // Heat
  if (ans.heat && ans.heat !== "any" && attrs.heat) {
    if (ans.heat === attrs.heat) score += 20;
    else if (ans.heat === "hybrid" && (attrs.heat === "traditional" || attrs.heat === "infrared")) score += 8; // partial match
    else score -= 10;
  }
  // Heater type preference
  if (ans.heater_type && ans.heater_type !== "any" && attrs.heater_type) {
    if (ans.heater_type === attrs.heater_type) score += 10; else score -= 5;
  }
  // Style
  if (ans.style && ans.style !== "any") {
    if (ans.style === attrs.style) score += 10; else score -= 5;
  }
  // Capacity
  if (ans.capacity && attrs.capacity) {
    const desired = ans.capacity;
    const cap = attrs.capacity;
    if (desired === "1" && cap === 1) score += 15;
    if (desired === "2" && cap === 2) score += 15;
    if (desired === "3-4" && (cap === 3 || cap === 4)) score += 15;
    if (desired === "5+" && cap >= 5) score += 15;
  }
  // Budget
  if (ans.budget && attrs.price) {
    const p = attrs.price;
    if (ans.budget === "<2000" && p < 2000) score += 15;
    if (ans.budget === "2000-5000" && p >= 2000 && p <= 5000) score += 15;
    if (ans.budget === "<3000" && p < 3000) score += 12;
    if (ans.budget === "3000-6000" && p >= 3000 && p <= 6000) score += 15;
    if (ans.budget === "6000-10000" && p > 6000 && p <= 10000) score += 15;
    if (ans.budget === ">10000" && p > 10000) score += 15;
  }
  // Power
  if (ans.power && attrs.power) {
    if (ans.power === attrs.power) score += 8;
  }
  // Basic presence boosts
  if (product.price_from != null) score += 3;
  if ((product.sections?.features || []).length) score += 2;
  if ((product.sections?.specifications || []).length) score += 2;
  return score;
}

function isWithinBudget(price, budget) {
  if (!Number.isFinite(price)) return false;
  if (!budget) return true;
  if (budget === "exploring") return true;
  if (budget === "<2000") return price < 2000;
  if (budget === "2000-5000") return price >= 2000 && price <= 5000;
  if (budget === "<3000") return price < 3000;
  if (budget === "3000-6000") return price >= 3000 && price <= 6000;
  if (budget === "6000-10000") return price > 6000 && price <= 10000;
  if (budget === ">10000") return price > 10000;
  return true;
}

async function collectCandidateProductsForQuiz(ans) {
  const tokens = [];
  if (ans.location) tokens.push(ans.location + " sauna");
  if (ans.heat && ans.heat !== "any") tokens.push(ans.heat + " sauna");
  if (ans.style && ans.style !== "any") tokens.push(ans.style + " sauna");
  if (ans.capacity) tokens.push(ans.capacity.includes("+") ? ans.capacity.replace("+", " person") + " sauna" : ans.capacity + " person sauna");
  // Also search for heater queries when user intent is generic
  const heaterTokens = [];
  heaterTokens.push("sauna heater");
  if (ans.heat && ans.heat !== "any") heaterTokens.push(`${ans.heat} sauna heater`);

  const handles = new Set();
  for (const t of [...tokens, ...heaterTokens]) {
    const hits = await predictiveSearch(t);
    for (const h of hits) handles.add(h.handle);
    const kw = await searchProductsByKeyword(t, 3, 10);
    for (const h of kw) handles.add(h.handle);
  }
  if (handles.size < 6) {
    const extra = await searchProductsByKeyword("sauna", 3, 10);
    for (const h of extra) handles.add(h.handle);
  }
  const products = [];
  for (const h of Array.from(handles).slice(0, 20)) {
    try {
      const p = await ensureProductByHandle(h);
      if (p) products.push(p);
    } catch {}
  }
  return products;
}
async function recommendTopFromQuiz(ans, maxResults = 3) {
  const productsRaw = await collectCandidateProductsForQuiz(ans);
  // Prefer sauna units; if none survive, fall back to non-heater products
  let products = productsRaw.filter(isLikelySaunaProduct);
  if (!products.length) {
    products = productsRaw.filter((p) => !/\bheater\b/i.test(String(p?.title || "")));
  }
  // Strict budget filtering if budget selected; if this wipes all, relax budget
  let budgeted = ans.budget
    ? products.filter((p) => isWithinBudget(p.price_from, ans.budget))
    : products;
  if (!budgeted.length) budgeted = products;
  const ranked = budgeted
    .map((p) => ({ p, score: scoreProductForQuiz(p, ans) }))
    .sort((a, b) => b.score - a.score)
    .slice(0, maxResults)
    .map(({ p, score }) => ({
      title: p.title,
      url: p.url,
      price: p.price_from_formatted || usd(p.price_from) || null,
      score,
    }));
  return ranked;
}

/* ====================== HELPERS ====================== */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const usd = (n) => (n && n > 0 ? `$${Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 })}` : null);
const usdCents = (cents) =>
  Number.isFinite(cents) ? `$${(cents / 100).toLocaleString("en-US", { maximumFractionDigits: 2 })}` : null;

/* ===== PLAIN TEXT ENFORCEMENT ===== */
const PLAIN_RULES = `
Formatting rules:
- Plain text only. No Markdown. No asterisks/bold/italics.
- Use simple hyphen bullets: "- Item".
- Write links as: Text - URL.
`;
const sys = (txt) => `${txt}\n${PLAIN_RULES}`;
function toPlainText(md = "") {
  let s = String(md);
  s = s.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, a, u) => `${a || "Image"} - ${u}`);
  s = s.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, t, u) => `${t} - ${u}`);
  s = s.replace(/(\*\*|__)(.*?)\1/g, "$2");
  s = s.replace(/(\*|_)(.*?)\1/g, "$2");
  s = s.replace(/~~(.*?)~~/g, "$1");
  s = s.replace(/`{1,3}([^`]+)`{1,3}/g, "$1");
  s = s.replace(/^\s{0,3}#{1,6}\s+/gm, "");
  s = s.replace(/^\s{0,3}>\s?/gm, "");
  s = s.replace(/^\s*-\s+/gm, "- ");
  s = s.replace(/\n{3,}/g, "\n\n");
  s = s.replace(/[ \t]+\n/g, "\n");
  return s.trim();
}

/* ===== TOKEN BUDGET HELPERS ===== */
const MAX = {
  VARIANTS: 3,
  SPECS: 50,
  FEATURES: 20,
  INCLUDED: 20,
  WARRANTY: 10,
  SHIPPING: 10,
  RETURNS: 10,
  FAQ: 0,
  TXT: 1200,
  USER: 1200,
  PROMPT: 18000
};
const clip = (s, n = MAX.TXT) => {
  if (s == null) return s;
  s = String(s).replace(/\s+/g, " ").trim();
  return s.length > n ? s.slice(0, n) + " …" : s;
};
const slimSpecs = (arr = [], max = MAX.SPECS) =>
  arr
    .filter(kv => kv && kv.key && kv.value)
    .slice(0, max)
    .map(({ key, value }) => ({ key: clip(key, 80), value: clip(value, 200) }));

function slimForIntent(product, intent = "general") {
  const S = product.sections || {};
  switch (intent) {
    case "features":  return { features: (S.features || []).slice(0, MAX.FEATURES).map(x => clip(x)) };
    case "specs":     return { specifications: slimSpecs(S.specifications || []) };
    case "size":      return { specifications: slimSpecs((S.specifications || []).filter(s => /dimension|interior|exterior|width|depth|height|volume|cubic\s*(feet|ft)/i.test(s.key))) };
    case "included":  return { whats_included: (S.whats_included || []).slice(0, MAX.INCLUDED).map(x => clip(x)) };
    case "warranty":  return { warranty: (S.warranty || []).slice(0, MAX.WARRANTY).map(x => clip(x)) };
    case "shipping":  return { shipping: (S.shipping || []).slice(0, MAX.SHIPPING).map(x => clip(x)) };
    case "returns":   return { returns: (S.returns || []).slice(0, MAX.RETURNS).map(x => clip(x)) };
    default:
      return {
        product_info: (S.product_info || []).slice(0, 3).map(x => clip(x, 700)),
        specifications: slimSpecs(S.specifications || []),
        features: (S.features || []).slice(0, MAX.FEATURES).map(x => clip(x)),
        whats_included: (S.whats_included || []).slice(0, MAX.INCLUDED).map(x => clip(x)),
        warranty: (S.warranty || []).slice(0, MAX.WARRANTY).map(x => clip(x)),
        shipping: (S.shipping || []).slice(0, MAX.SHIPPING).map(x => clip(x)),
        returns: (S.returns || []).slice(0, MAX.RETURNS).map(x => clip(x)),
      };
  }
}
function slimCtxSingle(product, intent = "general") {
  const slim = [{
    title: product.title,
    url: product.url,
    price_from: product.price_from,
    vendor: product.vendor,
    sections: slimForIntent(product, intent),
  }];
  let j = JSON.stringify(slim, null, 2);
  if (j.length > MAX.PROMPT) {
    const keep = ["product_info","specifications","features","whats_included"];
    slim[0].sections = Object.fromEntries(Object.entries(slim[0].sections).filter(([k]) => keep.includes(k)));
    j = JSON.stringify(slim, null, 2);
  }
  return j;
}

// Plain-text helper for adding a clickable-style link line
function productLinkLine(product, label = "Click here") {
  const url = product?.url || "";
  return url ? `${label} - ${url}` : "";
}

// Slim multiple products for comparison prompts
function slimCtxMulti(products = [], intent = "general") {
  const slim = products.map((product) => ({
    title: product.title,
    url: product.url,
    price_from: product.price_from,
    vendor: product.vendor,
    sections: slimForIntent(product, intent),
  }));
  let j = JSON.stringify(slim, null, 2);
  if (j.length > MAX.PROMPT) {
    // Drop some section types if too large
    for (let i = 0; i < slim.length; i++) {
      const keep = ["product_info","specifications","features"]; // tighter for multi
      slim[i].sections = Object.fromEntries(Object.entries(slim[i].sections || {}).filter(([k]) => keep.includes(k)));
    }
    j = JSON.stringify(slim, null, 2);
  }
  return j;
}

/* ====================== SCRAPER (SINGLE PRODUCT) ====================== */
function isSpecKey(k = "") {
  return /^(capacity|heater|heaters|power|watt|kw|amps?|voltage|electrical|interior|exterior|dimension|width|depth|height|size|volume|material|wood|door|glass|controls?|weight|shipping|timer|speaker|chromotherapy|light|temperature|warranty|window|bench)/i.test(
    k
  );
}
function kvFromLi($, li) {
  const $li = $(li);
  const strong = $li.find("strong,b").first().text().trim().replace(/\s+/g, " ");
  const text = $li.clone().children("strong,b,span:first").remove().end().text().trim().replace(/\s+/g, " ");
  if (strong) {
    const key = strong.replace(/[:：]+$/, "").trim();
    const value = text || $li.contents().not($li.children()).text().trim();
    if (key && value) return { key, value };
  }
  const spans = $li.find("span");
  if (spans.length >= 2) {
    const firstSpan = spans[0];
    const lastSpan = spans[spans.length - 1];
    const key = $(firstSpan).text().trim();
    const value = $(lastSpan).text().trim();
    if (key && value) return { key, value };
  }
  const raw = $li.text().trim().replace(/\s+/g, " ");
  const m = raw.match(/^(.{2,80}?):\s*(.+)$/);
  if (m) return { key: m[1].trim(), value: m[2].trim() };
  return null;
}
function pullAccordionBlock($, headingEl) {
  const $h = $(headingEl);
  const id = $h.attr("aria-controls") || $h.closest("[aria-controls]").attr("aria-controls");
  if (id) {
    const $panel = $("#" + id);
    if ($panel && $panel.length) return $panel;
  }
  const $acc = $h.closest(".accordion, .product__accordion, .collapsible-content, .tabs, .product-tabs, .tab-content, .accordion__item, details");
  if ($acc.length) {
    const nextPanel = $h.next(".accordion__content, .collapsible-content__inner, .product__accordion-content, .tab-panel, .tabs__panel, .content, .rte");
    if (nextPanel.length) return nextPanel;
    const inner = $acc.find(".accordion__content, .collapsible-content__inner, .product__accordion-content, .tab-panel, .tabs__panel, .rte").first();
    if (inner.length) return inner;
  }
  return null;
}

/* ====== Noise filters for product_info ====== */
const STORE_BOILER_RX =
  /(free shipping|lowest price|price match|call us|\bmon[-\s]?fri\b|reviews?\b|add to cart|choose an option|unit price|easy returns|why buy from us|family[-\s]owned|join our community|contact us|privacy policy|terms of service|legal terms|payment policy)/i;
const NON_PRODUCT_INFO_RX =
  /(copyright|©\s?\d{4}|phone|email|info@|@media|bundle-button|modal|script|style|newsletter|login|sign in|account|cart|cookies?)/i;

/* ======== EXTRA SPEC EXTRACTORS ======== */
function kvFromDl($, dl) {
  const out = [];
  $(dl).find("dt").each((i, dt) => {
    const key = $(dt).text().replace(/\s+/g, " ").trim();
    const dd = $(dt).next("dd");
    const value = dd.text().replace(/\s+/g, " ").trim();
    if (key && value) out.push({ key, value });
  });
  return out;
}
function kvFromTextLines(lines = []) {
  const out = [];
  for (let raw of lines) {
    if (!raw) continue;
    const line = raw.replace(/\s+/g, " ").trim();
    if (!line) continue;
    const m = line.match(/^([^:–-]{2,80}?)[\s]*[:–-][\s]*(.+)$/); // Key: Value | Key - Value | Key – Value
    if (m) {
      const key = m[1].trim();
      const value = m[2].trim();
      if (key && value) out.push({ key, value });
    }
  }
  return out;
}

/* ======== strip style/script + CSS pair filtering ======== */
function kvFromParagraphOrDiv($, el) {
  const $clone = $(el).clone();
  $clone.find("style,script,noscript,template").remove();
  let html = $clone.html() || "";
  const withBreaks = html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n")
    .replace(/<li>/gi, "\n")
    .replace(/<\/li>/gi, "\n");
  const text = withBreaks.replace(/<[^>]+>/g, " ");
  const lines = text.split(/\n|\r/).map(s => s.trim()).filter(Boolean);
  return kvFromTextLines(lines);
}
const CSS_PROP_RX = /^(width|height|max-|min-|margin|padding|left|right|top|bottom|display|position|z-index|background|color|font|line-height|letter-spacing|border|box-shadow|opacity|transform|transition|animation|overflow|grid|flex|align|justify|gap|object-fit|text|white-space|word|clip|visibility)$/i;
const CSS_VALUE_RX = /\b\d+(\.\d+)?(px|%|vw|vh|rem|em)\b|calc\(|!important|^var\(/i;
const MEASURE_RX = /\b\d+(\.\d+)?\s*(in(?:ches)?|cm|mm|ft|feet)\b|[""]/i;
function isLikelyCssPair(kv) {
  const key = (kv.key || "").trim();
  const val = (kv.value || "").trim();
  if (MEASURE_RX.test(val)) return false;
  if (CSS_PROP_RX.test(key)) return true;
  if ((/^width$|^height$/i.test(key)) && CSS_VALUE_RX.test(val)) return true;
  if (CSS_VALUE_RX.test(val)) return true;
  return false;
}
function filterOutCssPairs(arr = []) {
  return (arr || []).filter(kv => !isLikelyCssPair(kv));
}

function extractStrictProductInfo($, $root) {
  const headRx = /\b(product information|overview|about|description|details)\b/i;
  const heads = $root.find("h1,h2,h3,strong,b").filter((_, el) => headRx.test($(el).text()));
  const out = [];
  if (!heads.length) return out;
  const $h = heads.first();
  let el = $h.next();
  while (el.length && !/^(H1|H2|H3|STRONG|B)$/i.test(el[0].tagName)) {
    if (el.is("style,script,noscript,template,form,button,figure,table")) { el = el.next(); continue; }
    if (el.is("ul,ol")) break;
    if (el.is("p,div,section")) {
      const t = el
        .clone()
        .find("ul,ol,table,form,button,a,svg,style,script")
        .remove()
        .end()
        .text()
        .replace(/\s+/g, " ")
        .trim();
      if (t && !STORE_BOILER_RX.test(t) && !NON_PRODUCT_INFO_RX.test(t) && t.split(/\s+/).length >= 8) {
        out.push(t);
      }
    }
    el = el.next();
    if (out.length >= 3) break;
  }
  return out;
}

async function fetchProductUltra(handle) {
  const base = `https://${STORE_DOMAIN}/products/${handle}`;
  const res = await fetch(base);
  if (!res.ok) return null;

  const html = await res.text();
  const $ = load(html);

  const normText = (t = "") => t.replace(/\s+/g, " ").trim();
  const getText = (el) => normText($(el).text() || "");

  const sectionNames = [
    { key: "specifications", rx: /\b(specs|specification|specifications|technical|tech specs?)\b/i },
    { key: "features", rx: /\b(features?|key features|additional features|highlights?)\b/i },
    { key: "whats_included", rx: /\b(what'?s included|included|in the box|box contents?|package contents?)\b/i },
    { key: "warranty", rx: /\b(warranty|guarantee)\b/i },
    { key: "shipping", rx: /\b(shipping|delivery|lead[-\s]?time)\b/i },
    { key: "returns", rx: /\b(return|refund|exchange)\b/i },
    { key: "faq", rx: /\b(faq|questions?\s*&\s*answers?|frequently asked)\b/i },
    { key: "manuals", rx: /\b(manual|downloads?|documents?|spec sheet|installation|owner'?s?)\b/i },
    { key: "product_info", rx: /\b(product information|overview|about|description|summary|details)\b/i },
  ];

  const title =
    $("meta[property='og:title']").attr("content") ||
    $("h1, h1.product-title, .product__title").first().text().trim() ||
    $("title").text().trim() ||
    handle;

  // Shopify .js
  let js = null;
  try { js = await (await fetch(`${base}.js`)).json(); } catch {}
  const vendor = js?.vendor || null;
  const variants = (js?.variants || []).map((v) => ({
    id: v.id,
    title: v.title,
    sku: v.sku || null,
    barcode: v.barcode || null,
    available: !!v.available,
    price_cents: Number(v.price),
    compare_at_price_cents: Number(v.compare_at_price || 0),
    option1: v.option1,
    option2: v.option2,
    option3: v.option3,
    weight: v.weight,
    weight_unit: v.weight_unit,
  }));
  const priceFromCents = variants.length ? Math.min(...variants.map((v) => v.price_cents)) : null;
  const price_from = priceFromCents != null ? priceFromCents / 100 : null;

  // Roots (also include accordions/tabs)
  const DESC_SELECTORS = [
    ".product__description",
    "[data-product-description]",
    ".product-single__description",
    ".product__accordion", ".accordion", ".collapsible-content",
    ".tabs", ".product-tabs", ".tab-content", ".tabs__panel",
    ".product-v2", ".product-v2-desc", ".product-v2-tab-content",
    ".product-template--tabs",
    ".rte", "main"
  ].join(", ");
  const descRoot = $(DESC_SELECTORS).first();

  const sections = {
    product_info: [],
    specifications: [],
    features: [],
    whats_included: [],
    warranty: [],
    shipping: [],
    returns: [],
    faq: [],
    manuals: [],
  };

  // STRICT Product Information
  sections.product_info = extractStrictProductInfo($, descRoot);

  // Parse headings (flat or accordion/tab content)
  const nodes = descRoot.find("h1,h2,h3,strong,b").filter((_, el) => getText(el).length > 0);
  const handleList = (list, mode) => {
    if (mode === "specifications") {
      $(list).find("li").each((_, li) => {
        const kv = kvFromLi($, li);
        if (kv) sections.specifications.push(kv);
      });
    } else if (mode === "whats_included") {
      $(list).find("li").each((_, li) => { const t = getText(li); if (t) sections.whats_included.push(t); });
    } else if (mode === "features") {
      $(list).find("li").each((_, li) => { const t = getText(li); if (t) sections.features.push(t); });
    }
  };

  if (nodes.length) {
    nodes.each((_, h) => {
      const label = getText(h);
      const sn = sectionNames.find((s) => s.rx.test(label))?.key || "product_info";
      const acc = pullAccordionBlock($, h);

      if (acc && acc.length) {
        acc.find("ul,ol").each((_, list) => handleList(list, sn));
        acc.find("table").each((_, t) => {
          $(t).find("tr").each((_, tr) => {
            const tds = $(tr).find("th,td");
            if (tds.length >= 2) {
              const key = getText(tds[0]);
              const value = getText(tds[1]);
              if (key && value) sections.specifications.push({ key, value });
            }
          });
        });
        if (sn !== "product_info" && sn !== "specifications") {
          const txt = getText(acc);
          if (txt && txt.length > 30) sections[sn].push(txt);
        }
        return;
      }

      // Flat DOM: walk siblings until next heading
      let el = $(h).next();
      while (el.length && !/^(H1|H2|H3|STRONG|B)$/i.test(el[0].tagName)) {
        if (sn === "product_info" && el.is("ul,ol,table")) break;
        if (el.is("ul,ol")) handleList(el, sn);
        else if (el.is("table")) {
          el.find("tr").each((_, tr) => {
            const tds = $(tr).find("th,td");
            if (tds.length >= 2) {
              const key = getText(tds[0]);
              const value = getText(tds[1]);
              if (key && value) sections.specifications.push({ key, value });
            }
          });
        } else {
          const t = normText(el.text());
          if (t && t.length > 6 && sn !== "product_info" && sn !== "specifications") sections[sn].push(t);
        }
        el = el.next();
      }
    });
  }

  // Fallback mining for unlabeled specs/included
  descRoot.find("ul,ol").each((_, list) => {
    const $list = $(list);
    $list.find("li").each((_, li) => {
      const kv = kvFromLi($, li);
      if (kv && isSpecKey(kv.key)) sections.specifications.push(kv);
    });
    const headerText = ($list.prev("h2,h3,strong,b").text() || "").toLowerCase();
    if (/included|in the box|box contents|package contents/.test(headerText)) {
      $list.find("li").each((_, li) => {
        const t = getText(li);
        if (t) sections.whats_included.push(t);
      });
    }
  });

  /* EXTRA: tables, dl, colon/dash lines (CSS filtering) */
  descRoot.find("table").each((_, t) => {
    $(t).find("tr").each((__, tr) => {
      const tds = $(tr).find("th,td");
      if (tds.length >= 2) {
        const key = getText(tds[0]);
        const value = getText(tds[1]);
        if (key && value) sections.specifications.push({ key, value });
      }
    });
  });
  descRoot.find("dl").each((_, dl) => {
    sections.specifications.push(...kvFromDl($, dl));
  });
  descRoot.find("p,div").each((_, el) => {
    let kvs = kvFromParagraphOrDiv($, el).filter(kv => isSpecKey(kv.key));
    kvs = filterOutCssPairs(kvs);
    sections.specifications.push(...kvs);
  });
  $('script[type="application/ld+json"]').each((_, s) => {
    try {
      const j = JSON.parse($(s).text());
      const nodes = Array.isArray(j) ? j : j["@graph"] || [j];
      for (const n of nodes) {
        const d = (n && typeof n === "object" && n.description) ? String(n.description) : null;
        if (!d) continue;
        const lines = d.split(/\n|\r/).map(x => x.trim()).filter(Boolean);
        let kvs = kvFromTextLines(lines).filter(kv => isSpecKey(kv.key));
        kvs = filterOutCssPairs(kvs);
        sections.specifications.push(...kvs);
      }
    } catch {}
  });

  // Dedupes + filters
  const dedupeKV = (arr) => {
    const seen = new Set();
    const out = [];
    for (const kv of arr) {
      if (!kv || !kv.key || !kv.value) continue;
      const sig = `${kv.key}=${kv.value}`.toLowerCase();
      if (!seen.has(sig)) { seen.add(sig); out.push(kv); }
    }
    return out;
  };
  const dedupe = (arr) =>
    Array.from(new Set(arr.map((x) => (typeof x === "string" ? x.trim() : JSON.stringify(x))))).map((s) =>
      s.startsWith("{") ? JSON.parse(s) : s
    );

  sections.product_info = (sections.product_info || [])
    .map(t => t.replace(/\s+/g, " ").trim())
    .filter(t => t && !STORE_BOILER_RX.test(t) && !NON_PRODUCT_INFO_RX.test(t))
    .slice(0, 3);

  sections.specifications = filterOutCssPairs(dedupeKV(sections.specifications));
  sections.features = dedupe(sections.features);
  sections.whats_included = dedupe(sections.whats_included);
  sections.warranty = dedupe(sections.warranty);
  sections.shipping = dedupe(sections.shipping);
  sections.returns = dedupe(sections.returns);
  sections.faq = [];

  const text_all = [
    title,
    sections.product_info.join(" "),
    sections.features.join(" "),
    sections.whats_included.join(" "),
    sections.warranty.join(" "),
    sections.shipping.join(" "),
    sections.returns.join(" "),
    sections.specifications.map((x) => `${x.key} ${x.value}`).join(" "),
  ].join(" ");

  return {
    title,
    handle,
    url: base,
    vendor,
    price_from: Number.isFinite(price_from) ? price_from : null,
    price_from_formatted: priceFromCents ? usdCents(priceFromCents) : price_from ? usd(price_from) : null,
    variants,
    sections,
    _hay_all: text_all.toLowerCase(),
  };
}

/* ====================== LOOKUPS & CACHE ====================== */
const CACHE = new Map();
const CACHE_LIMIT = 1000;

function cacheSetLimited(handle, prod) {
  if (CACHE.size >= CACHE_LIMIT) {
    const firstKey = CACHE.keys().next().value;
    if (firstKey) CACHE.delete(firstKey);
  }
  CACHE.set(handle, prod);
}

const slugify = (s) =>
  s
    .toLowerCase()
    .replace(/™|®|©/g, "")
    .replace(/[^a-z0-9\s-]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
    .replace(/\s/g, "-")
    .replace(/-+/g, "-");

async function predictiveSearch(q) {
  try {
    const url =
      `https://${STORE_DOMAIN}/search/suggest.json` +
      `?q=${encodeURIComponent(q)}` +
      `&resources[type]=product&resources[limit]=6&section_id=predictive-search`;
    const r = await fetch(url, { headers: { accept: "application/json" } });
    if (!r.ok) return [];
    const j = await r.json();
    const products = j?.resources?.results?.products || [];
    return products.map((p) => ({ handle: p.handle, title: p.title }));
  } catch { return []; }
}

async function searchAllProductsFor(keyword, maxPages = 5) {
  const kw = keyword.toLowerCase();
  for (let page = 1; page <= maxPages; page++) {
    const url = `https://${STORE_DOMAIN}/products.json?limit=250&page=${page}`;
    const r = await fetch(url);
    if (!r.ok) break;
    const j = await r.json();
    const items = j?.products || [];
    if (!items.length) break;
    const hit = items.find(
      (p) =>
        (p.title && p.title.toLowerCase().includes(kw)) ||
        (p.handle && p.handle.toLowerCase().includes(slugify(keyword)))
    );
    if (hit) return hit.handle;
    if (items.length < 250) break;
    await sleep(200);
  }
  return null;
}

// Find multiple products by keyword(s)
async function searchProductsByKeyword(keyword, maxPages = 5, maxResults = 8) {
  const kw = keyword.toLowerCase();
  const words = kw.split(/\s+/).filter(Boolean);
  const results = [];
  const seen = new Set();
  for (let page = 1; page <= maxPages; page++) {
    const url = `https://${STORE_DOMAIN}/products.json?limit=250&page=${page}`;
    const r = await fetch(url, { headers: { accept: "application/json" } });
    if (!r.ok) break;
    const j = await r.json();
    const items = j?.products || [];
    if (!items.length) break;
    for (const p of items) {
      const title = (p.title || "").toLowerCase();
      const handle = (p.handle || "").toLowerCase();
      const match = words.every((w) => title.includes(w) || handle.includes(slugify(w)));
      if (match && !seen.has(handle)) {
        seen.add(handle);
        results.push({ handle: p.handle, title: p.title });
        if (results.length >= maxResults) return results;
      }
    }
    if (items.length < 250) break;
    await sleep(100);
  }
  return results;
}

// Fuzzy search: match if ANY word appears in title/handle; rank by matches
const STOP_WORDS = new Set([
  "what", "whats", "what's", "with", "for", "on", "about", "of", "the", "and", "&",
  "size", "sizes", "dimensions", "dimension", "specs", "spec", "included", "include",
  "warranty", "shipping", "returns", "return", "lead", "time", "leadtime"
]);
async function searchProductsByAnyWord(keyword, maxPages = 5, maxResults = 8) {
  const kw = String(keyword || "").toLowerCase();
  const words = Array.from(new Set(
    kw
      .split(/[^a-z0-9+.-]+/i)
      .map(w => w.trim())
      .filter(w => w && w.length >= 2 && !STOP_WORDS.has(w))
  ));
  if (!words.length) return [];
  const scored = [];
  const seen = new Set();
  for (let page = 1; page <= maxPages; page++) {
    const url = `https://${STORE_DOMAIN}/products.json?limit=250&page=${page}`;
    const r = await fetch(url, { headers: { accept: "application/json" } });
    if (!r.ok) break;
    const j = await r.json();
    const items = j?.products || [];
    if (!items.length) break;
    for (const p of items) {
      const title = (p.title || "").toLowerCase();
      const handle = (p.handle || "").toLowerCase();
      let score = 0;
      for (const w of words) {
        if (title.includes(w) || handle.includes(slugify(w))) score++;
      }
      if (score > 0 && !seen.has(handle)) {
        seen.add(handle);
        scored.push({ handle: p.handle, title: p.title, score });
      }
    }
    if (items.length < 250) break;
    await sleep(80);
  }
  scored.sort((a, b) => b.score - a.score || a.title.length - b.title.length);
  return scored.slice(0, maxResults);
}

async function ensureProductByHandle(handle) {
  if (CACHE.has(handle)) return CACHE.get(handle);
  const prod = await fetchProductUltra(handle);
  if (prod) cacheSetLimited(handle, prod);
  return prod;
}

function extractProductQuery(msg) {
  const rx = /\b(price|cost|specs?|specifications?|features?|overview|summary|information|info|details?|size|dimensions?|warranty|lead\s*time|shipping|delivery|returns?)\b.*?\b(for|on|about|regarding|of)\b\s+(.+)/i;
  const m = msg.match(rx);
  if (m?.[3]) return m[3].trim();
  return msg.trim();
}

async function resolveRequestedProduct(userMsg) {
  const urlRe = new RegExp(`https?://[^\\s]*${STORE_DOMAIN.replace(/\./g, "\\.")}/products/([a-z0-9-]+)`, "i");
  const m = userMsg.match(urlRe);
  if (m?.[1]) return m[1];

  const phrase = extractProductQuery(userMsg);
  const hits = await predictiveSearch(phrase);
  if (hits.length) return hits[0].handle;

  const guesses = new Set();
  const full = slugify(phrase);
  guesses.add(full);
  const cut = phrase.split(/\s(?:with|–|-|:)\s/i)[0];
  if (cut && cut.length > 6) guesses.add(slugify(cut));
  for (const g of guesses) {
    const test = await fetch(`https://${STORE_DOMAIN}/products/${g}.js`);
    if (test.ok) return g;
  }

  return await searchAllProductsFor(phrase, 5);
}

// Loose resolver using any-word fuzzy match as last fallback
async function resolveRequestedProductLoose(userMsg) {
  const phrase = extractProductQuery(userMsg);
  // Try any-word matching against catalog
  const anyHits = await searchProductsByAnyWord(phrase, 5, 5);
  if (anyHits.length) return anyHits[0].handle;
  // Try a trimmed phrase before punctuation/"and"
  const cut = phrase.split(/[,;]|\band\b|\bwith\b|\bfor\b/i)[0];
  if (cut && cut.trim().length > 3) {
    const hits2 = await searchProductsByAnyWord(cut.trim(), 5, 5);
    if (hits2.length) return hits2[0].handle;
  }
  return null;
}

/* ====================== FULL DUMP HELPERS ====================== */
const SECTION_KEYWORDS_RX =
  /\b(price|cost|specs?|specifications?|features?|overview|summary|dimension|size|interior|exterior|width|depth|height|volume|included|what'?s included|warranty|shipping|delivery|lead[-\s]?time|returns?|refund|exchange|faq|questions?|manuals?|pdf|install|spec sheet|wiring|variant|sku|model|option)\b/i;

function formatKVList(arr = []) {
  return arr.map(kv => `${kv.key}: ${kv.value}`).join("\n");
}
function formatBullets(arr = []) {
  return arr.map(x => `- ${typeof x === "string" ? x : JSON.stringify(x)}`).join("\n");
}

// Extract manuals/installation PDFs/links from sections text blobs
function extractManualLinks(product) {
  const links = [];
  const pushIf = (txt) => {
    const urlRx = /(https?:\/\/[^\s)]+\.(?:pdf|PDF|docx?|DOCX?))/g;
    let m;
    while ((m = urlRx.exec(txt))) links.push(m[1]);
  };
  const S = product.sections || {};
  for (const key of ["manuals", "product_info", "features", "shipping", "warranty", "returns"]) {
    const arr = Array.isArray(S[key]) ? S[key] : [];
    for (const t of arr) pushIf(String(t));
  }
  // Also scan specs values
  for (const kv of (S.specifications || [])) pushIf(`${kv.key}: ${kv.value}`);
  return Array.from(new Set(links)).slice(0, 10);
}

// Try to fetch contact details (phone/email) from common pages/footer
async function fetchContactDetails() {
  const bases = [
    `https://${STORE_DOMAIN}/pages/contact`,
    `https://${STORE_DOMAIN}/contact`,
    `https://${STORE_DOMAIN}/pages/support`,
    `https://${STORE_DOMAIN}/pages/customer-service`,
    `https://${STORE_DOMAIN}/`,
  ];
  let phone = null;
  let email = null;
  let urlUsed = null;
  for (const url of bases) {
    try {
      const res = await fetch(url, { headers: { accept: "text/html,application/xhtml+xml" } });
      if (!res.ok) continue;
      const html = await res.text();
      const $ = load(html);
      const scope = $('footer, .site-footer, #footer').first();
      const root = scope.length ? scope : $('body');
      // Prefer explicit tel/mailto anchors
      const telHref = root.find('a[href^="tel:"]').first().attr('href');
      const mailHref = root.find('a[href^="mailto:"]').first().attr('href');
      if (telHref && !phone) phone = String(telHref).replace(/^tel:/i, '').trim();
      if (mailHref && !email) email = String(mailHref).replace(/^mailto:/i, '').trim();
      const text = root.text();
      if (!email) {
        const m = text.match(/(?:^|[^A-Z0-9._%+-])([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})/i);
        if (m) email = m[1];
      }
      if (!phone) {
        const p = text.match(/(?:\(\d{3}\)|\+?\d)[\d\s().-]{6,}\d/);
        if (p) phone = p[0].replace(/\s+/g, ' ').trim();
      }
      if (!urlUsed && (phone || email)) urlUsed = url;
      if (phone && email) break;
    } catch {}
  }
  return { phone, email, url: urlUsed || bases[0] };
}

/* Print EVERYTHING (no overview, no FAQ) */
function formatProductFull(product) {
  const s = product.sections || {};
  const parts = [];

  const price = product.price_from_formatted || usd(product.price_from);

  parts.push(`${product.title}`);
  parts.push(`${product.url}`);
  if (price) parts.push(`Price from: ${price}`);

  if ((s.product_info || []).length) {
    parts.push("", "Product Information");
    parts.push(formatBullets(s.product_info));
  }

  if ((s.features || []).length) {
    parts.push("", "Features");
    parts.push(formatBullets(s.features));
  }
  if ((s.specifications || []).length) {
    parts.push("", "Specifications");
    parts.push(formatKVList(s.specifications));
  }
  if ((s.whats_included || []).length) {
    parts.push("", "What's Included");
    parts.push(formatBullets(s.whats_included));
  }
  if ((s.warranty || []).length) {
    parts.push("", "Warranty");
    parts.push(formatBullets(s.warranty));
  }
  if ((s.shipping || []).length) {
    parts.push("", "Shipping / Lead Time");
    parts.push(formatBullets(s.shipping));
  }
  if ((s.returns || []).length) {
    parts.push("", "Returns");
    parts.push(formatBullets(s.returns));
  }
  return parts.join("\n");
}

function formatProductInfoOnly(product) {
  const arr = product.sections?.product_info || [];
  return arr.length ? arr.map(p => `- ${p}`).join("\n") : "No product information available.";
}

// Concise overview for product name queries
function formatProductOverview(product) {
  const attrs = inferProductAttributes(product);
  const locationLabel = attrs.location ? `${attrs.location} ` : "";
  const header = `Here is an overview of the first ${locationLabel}sauna option I found:`;

  const s = product.sections || {};
  const bullets = [];

  // Prefer features
  for (const f of (s.features || [])) {
    if (bullets.length >= 6) break;
    const line = String(f).replace(/^[-•\s]+/, "").trim();
    if (line) bullets.push(`- ${clip(line, 180)}`);
  }
  // Then product info
  if (bullets.length < 6) {
    for (const p of (s.product_info || [])) {
      if (bullets.length >= 6) break;
      const line = String(p).replace(/^[-•\s]+/, "").trim();
      if (line) bullets.push(`- ${clip(line, 180)}`);
    }
  }
  // Then a few key specs if still short
  if (bullets.length < 6) {
    const specs = (s.specifications || []).filter(kv => kv && kv.key && kv.value);
    const preferred = specs.filter(kv => /capacity|people|material|wood|dimension|width|depth|height|roof|glass|door|bench|lighting|warranty/i.test(`${kv.key} ${kv.value}`));
    for (const kv of preferred.concat(specs)) {
      if (bullets.length >= 6) break;
      const text = `${kv.key}: ${kv.value}`.replace(/^[-•\s]+/, "").trim();
      if (text) bullets.push(`- ${clip(text, 180)}`);
    }
  }

  const lines = [];
  lines.push(header, "", product.title, "");
  if (product.url) lines.push(`Click here - ${product.url}`, "");
  if (bullets.length) lines.push(...bullets);
  lines.push("", "Would you like more details, pricing, or information about installation for this model?");
  return lines.join("\n");
}

/* ====================== VECTOR INDEXING STUB ====================== */
/*
  Replace this with your embeddings + vector DB upsert.
  Example flow:
    - const chunks = makeChunks(product);
    - const vectors = await embed(chunks.map(c => c.text));
    - await qdrant.upsert({ points: chunks.map((c, i) => ({ id, vector: vectors[i], payload: c.meta })) });
*/
async function indexProduct(product) {
  // No-op placeholder; return true to signal success
  return true;
}

/* ====================== TIMEOUT + RETRY HELPERS ====================== */
function withTimeout(promise, ms, label = "task") {
  return new Promise((resolve, reject) => {
    const id = setTimeout(() => reject(new Error(`${label} timed out after ${ms}ms`)), ms);
    promise
      .then((v) => { clearTimeout(id); resolve(v); })
      .catch((e) => { clearTimeout(id); reject(e); });
  });
}
async function withRetry(fn, retries = INGEST_RETRIES, label = "task") {
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      return await fn();
    } catch (e) {
      lastErr = e;
      if (i < retries) await sleep(300 + i * 300);
    }
  }
  throw lastErr || new Error(`${label} failed`);
}

/* ====================== BULK INGEST STATE ====================== */
const BULK = {
  running: false,
  cancelled: false,
  total: 0,
  done: 0,
  failed: 0,
  queue: [],
  inFlight: new Set(),
  errors: [],
  startedAt: null,
  endedAt: null,
};

/** Fetch all product handles via Storefront JSON paging */
async function listAllProductHandles(maxPages = 50) {
  const handles = [];
  for (let page = 1; page <= maxPages; page++) {
    const url = `https://${STORE_DOMAIN}/products.json?limit=250&page=${page}`;
    const r = await fetch(url, { headers: { "accept": "application/json" } });
    if (!r.ok) break;
    const j = await r.json();
    const items = j?.products || [];
    if (!items.length) break;
    for (const p of items) if (p?.handle) handles.push(p.handle);
    if (items.length < 250) break;
    await sleep(100);
  }
  return Array.from(new Set(handles));
}

async function ingestHandleOnce(handle) {
  const prod = await withTimeout(ensureProductByHandle(handle), INGEST_TIMEOUT_MS, `fetch ${handle}`);
  if (!prod) throw new Error(`No product for ${handle}`);
  cacheSetLimited(handle, prod);
  await withTimeout(indexProduct(prod), INGEST_TIMEOUT_MS, `index ${handle}`);
  return true;
}

async function runBulkIngest(handles = []) {
  BULK.running = true;
  BULK.cancelled = false;
  BULK.total = handles.length;
  BULK.done = 0;
  BULK.failed = 0;
  BULK.errors = [];
  BULK.queue = handles.slice();
  BULK.inFlight.clear();
  BULK.startedAt = Date.now();
  BULK.endedAt = null;

  let active = 0;

  async function next() {
    if (BULK.cancelled) return;
    if (!BULK.queue.length) return;
    if (active >= INGEST_CONCURRENCY) return;

    const handle = BULK.queue.shift();
    active++;
    BULK.inFlight.add(handle);

    (async () => {
      try {
        await withRetry(() => ingestHandleOnce(handle), INGEST_RETRIES, `ingest ${handle}`);
        BULK.done++;
      } catch (e) {
        BULK.failed++;
        BULK.errors.push({ handle, error: String(e?.message || e) });
      } finally {
        active--;
        BULK.inFlight.delete(handle);
        if (!BULK.cancelled && BULK.queue.length && active < INGEST_CONCURRENCY) {
          await sleep(INGEST_BATCH_DELAY_MS);
          next();
        }
        while (!BULK.cancelled && BULK.queue.length && active < INGEST_CONCURRENCY) next();
      }
    })();
  }

  for (let i = 0; i < Math.min(INGEST_CONCURRENCY, BULK.queue.length); i++) next();

  while (!BULK.cancelled && (BULK.done + BULK.failed < BULK.total || active > 0)) {
    await sleep(200);
  }
  BULK.running = false;
  BULK.endedAt = Date.now();
  return {
    total: BULK.total,
    done: BULK.done,
    failed: BULK.failed,
    duration_ms: (BULK.endedAt || Date.now()) - (BULK.startedAt || Date.now()),
    errors: BULK.errors.slice(0, 20),
  };
}

/* ====================== ROUTES ====================== */
app.get("/", (_req, res) =>
  res.send("SaunaBot up (text-only; no overview/faq; specs fixed) — bulk ingest ready")
);

app.get("/debug", (_req, res) => {
  res.json({
    cached_handles: Array.from(CACHE.keys()),
    cached_count: CACHE.size,
    last_product_title: LAST_PRODUCT?.title || null,
    last_product_handle: LAST_PRODUCT?.handle || null
  });
});

app.get("/ingest/:handle", async (req, res) => {
  try {
    const prod = await ensureProductByHandle(req.params.handle);
    if (!prod) return res.status(404).json({ error: "Not found" });
    LAST_PRODUCT = prod;
    res.json({ ok: true, handle: req.params.handle, cached: CACHE.has(req.params.handle) });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

/* ====================== BULK INGEST ROUTES ====================== */

// Kick off: GET /ingest/all  or  /ingest/all?handles=a,b,c  or  /ingest/all?limit=150
app.get("/ingest/all", async (req, res) => {
  try {
    if (BULK.running) return res.status(409).json({ error: "Bulk ingest already running" });

    const queryHandles = String(req.query.handles || "").split(",").map(s => s.trim()).filter(Boolean);
    let handles = queryHandles.length ? queryHandles : await listAllProductHandles();

    const limit = Number(req.query.limit || 0);
    if (limit && limit > 0) handles = handles.slice(0, limit);

    if (!handles.length) return res.status(404).json({ error: "No products found" });

    runBulkIngest(handles).catch(() => { /* tracked in BULK */ });

    res.status(202).json({
      ok: true,
      message: `Started bulk ingest of ${handles.length} products`,
      concurrency: INGEST_CONCURRENCY,
      timeout_ms: INGEST_TIMEOUT_MS,
      retries: INGEST_RETRIES,
      batch_delay_ms: INGEST_BATCH_DELAY_MS,
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// Live status
app.get("/ingest/status", (_req, res) => {
  res.json({
    running: BULK.running,
    cancelled: BULK.cancelled,
    total: BULK.total,
    done: BULK.done,
    failed: BULK.failed,
    inFlight: Array.from(BULK.inFlight),
    queued: BULK.queue.length,
    startedAt: BULK.startedAt,
    endedAt: BULK.endedAt,
    errors_preview: BULK.errors.slice(-10),
  });
});

// Cancel job
app.post("/ingest/cancel", (_req, res) => {
  if (!BULK.running) return res.json({ ok: true, message: "No bulk job running" });
  BULK.cancelled = true;
  res.json({ ok: true, message: "Cancellation requested" });
});

/* ====================== CHAT ====================== */
app.post("/chat", async (req, res) => {
  try {
    const userMsgRaw = (req.body?.message || "").trim();
    if (!userMsgRaw) {
      const payload = { ts: Date.now(), type: "chat", user: userMsgRaw, reply: null, error: "message required" };
      logChatResponse(payload);
      return res.status(400).json({ error: "message required" });
    }
    const userMsg = clip(userMsgRaw, MAX.USER);

    // Establish session key early for identity checks
    const sessionKey = getSessionKey(req);
    cleanupUserSessions();

    // Greetings
    const isGreeting = /\b(hi|hello|hey|yo|howdy|greetings|good\s*(morning|afternoon|evening))\b/i.test(userMsgRaw) && userMsgRaw.length <= 40;
    if (isGreeting) {
      const ident = ensureUserIdentity(sessionKey, userMsgRaw);
      if (ident.justCompleted) {
        QUIZ_SESSIONS.delete(sessionKey);
        LAST_PRODUCT = null;
        const reply = `Hello ${ident.name},  I'm here to help you today. What can I assist you with?`;
        const quick = quickRepliesForGreeting();
        logChatResponse({ ts: Date.now(), type: "chat", user: "[redacted: identity submit]", reply, handle: null });
        return res.json({ reply, quick_replies: quick });
      }
      if (!ident.ok) {
        logChatResponse({ ts: Date.now(), type: "chat", user: "[redacted: identity prompt]", reply: ident.reply, handle: null });
        return res.json({ reply: ident.reply });
      }
      // Reset session state on greeting with known identity
      QUIZ_SESSIONS.delete(sessionKey);
      LAST_PRODUCT = null;
      const reply = `Hello ${ident.name},  I'm here to help you today. What can I assist you with?`;
      const quick = quickRepliesForGreeting();
      logChatResponse({ ts: Date.now(), type: "chat", user: "[redacted: greeting]", reply, handle: null });
      return res.json({ reply, quick_replies: quick });
    }

    // Require identity before any other flow
    const ident = ensureUserIdentity(sessionKey, userMsgRaw);
    if (ident.justCompleted) {
      // Reset session state after identity submission to start fresh
      QUIZ_SESSIONS.delete(sessionKey);
      LAST_PRODUCT = null;
      const reply = `Hello ${ident.name},  I'm here to help you today. What can I assist you with?`;
      const quick = quickRepliesForGreeting();
      logChatResponse({ ts: Date.now(), type: "chat", user: "[redacted: identity submit]", reply, handle: null });
      return res.json({ reply, quick_replies: quick });
    }
    if (!ident.ok) {
      logChatResponse({ ts: Date.now(), type: "chat", user: "[redacted: identity prompt]", reply: ident.reply, handle: null });
      return res.json({ reply: ident.reply });
    }

    // QUICK ACTIONS: handle before hard guard
    {
      const act = parseGreetingAction(userMsgRaw);
      if (act === "CONTACT"){
        const { phone, email, url } = await fetchContactDetails();
        const out = ["Here is our support contact:"];
        if (phone) out.push(`Phone: ${phone}`);
        if (email) out.push(`Email: ${email}`);
        out.push(`Click here - ${url}`);
        const reply = out.join("\n");
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }
      if (act === "SHIPPING") {
        const reply = "Tell me the product or paste a link and I'll show its shipping/lead-time details.";
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }
    }

    // HARD GUARD: Only handle messages that explicitly mention the word "sauna"
    {
      const mentionsSauna = /\bsauna\b/i.test(userMsgRaw);
      const hasActiveQuiz = !!QUIZ_SESSIONS.get(sessionKey);
      const isQuizCmd = /\b(start\s*quiz|quiz|survey)\b/i.test(userMsgRaw);
      // Allow direct contact support requests, shipping/lead time, warranty/returns to bypass guard
      const wantsContact = /(contact|support|help\s*desk|customer\s*service)/i.test(userMsgRaw);
      const wantsShippingLead = /(delivery|lead\s*time|shipping)/i.test(userMsgRaw);
      const wantsWarrantyReturns = /(warranty|returns?|refund|exchange|installation|install)/i.test(userMsgRaw);
      if (!mentionsSauna && !hasActiveQuiz && !isQuizCmd && !wantsContact && !wantsShippingLead && !wantsWarrantyReturns) {
        const { phone, email, url } = await fetchContactDetails();
        const lines = [
          "I can answer sauna-related questions only.",
          "For other help, please contact our support team:",
        ];
        if (phone) lines.push(`Phone: ${phone}`);
        if (email) lines.push(`Email: ${email}`);
        lines.push(`Click here - ${url}`);
        const reply = lines.join("\n");
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }
      if (wantsContact) {
        const { phone, email, url } = await fetchContactDetails();
        const lines = ["Here is our support contact:"];
        if (phone) lines.push(`Phone: ${phone}`);
        if (email) lines.push(`Email: ${email}`);
        lines.push(`Click here - ${url}`);
        const reply = lines.join("\n");
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }
    }

    // QUIZ FLOW (PRIORITIZED BEFORE PRODUCT LOOKUPS)
    {
      // Handle greeting action buttons quickly
      const act = parseGreetingAction(userMsgRaw);
      if (act === "CONTACT"){
        const { phone, email, url } = await fetchContactDetails();
        const out = ["Here is our support contact:"];
        if (phone) out.push(`Phone: ${phone}`);
        if (email) out.push(`Email: ${email}`);
        out.push(`Click here - ${url}`);
        const reply = out.join("\n");
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }
      if (act === "QUIZ"){
        QUIZ_SESSIONS.delete(sessionKey);
        const domain = detectQuizDomainFromText(userMsgRaw);
        const state = { stepIndex: 0, answers: {}, createdAt: Date.now(), domain, customFirst: shouldAskCustomFirstFromText(domain, userMsgRaw) };
        QUIZ_SESSIONS.set(sessionKey, state);
          const reply = [
            `Let's find your best match with a quick ${QUIZ_COUNT}-question quiz.`,
            formatQuizQuestion(0),
          ].join("\n\n");
        const quick = quickRepliesForStep(0);
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply, quick_replies: quick });
      }
      cleanupQuizSessions();
      const genericHeater = /\b(sauna heater|heater)\b/i.test(userMsg) && !/https?:\/\//i.test(userMsg);
      const wantsRecGeneric = /\b(best|top|good|recommend(?:ed)?|suggest|looking for|find|options?)\b/i.test(userMsg);
      const justSauna = /^\s*saunas?\s*\??\s*$/i.test(userMsgRaw);
      const askForQuiz = /\b(best.*for me|help.*choose|recommend.*for me|which.*for me|quiz|survey)\b/i.test(userMsg)
        || genericHeater
        || wantsRecGeneric
        || justSauna;
      const isStartQuiz = /\b(start quiz|quiz|survey)\b/i.test(userMsg);
      const isResetQuiz = /\b(reset quiz|restart quiz|cancel quiz|stop quiz)\b/i.test(userMsg);

      if (isResetQuiz) {
        QUIZ_SESSIONS.delete(sessionKey);
        const reply = "Quiz reset. Say 'start quiz' to begin again.";
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }

      let state = QUIZ_SESSIONS.get(sessionKey);
      // If a quiz is in progress but the user appears to ask a different, product-specific question,
      // end the quiz and proceed with normal handling.
      if (state && !isStartQuiz && !isResetQuiz) {
        const looksLikeNonQuiz = SECTION_KEYWORDS_RX.test(userMsgRaw)
          || /https?:\/\//i.test(userMsgRaw)
          || /\b([A-Z]{1,4}-?\d{1,4}[A-Z0-9-]*)\b/i.test(userMsgRaw)
          || /\bcompare|vs\.?\b/i.test(userMsgRaw);
        if (looksLikeNonQuiz) {
          QUIZ_SESSIONS.delete(sessionKey);
          state = null;
        }
      }
      if (isStartQuiz || (!state && askForQuiz)) {
        const domain = detectQuizDomainFromText(userMsgRaw);
        state = { stepIndex: 0, answers: {}, createdAt: Date.now(), domain, customFirst: shouldAskCustomFirstFromText(domain, userMsgRaw) };
        QUIZ_SESSIONS.set(sessionKey, state);
        const pre = prefillQuizAnswersFromText(userMsgRaw);
        Object.assign(state.answers, pre);
        while (state.stepIndex < QUIZ_QUESTIONS.length && state.answers[QUIZ_QUESTIONS[state.stepIndex].id]) {
          state.stepIndex++;
        }
        if (state.customFirst) {
          const reply = [
            `Let's find your best match with a quick ${QUIZ_COUNT}-question quiz.`,
            formatCustomFirstQuestion(state.domain),
          ].join("\n\n");
          const quick = quickRepliesForCustom(state.domain);
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
          return res.json({ reply, quick_replies: quick });
        }
        if (state.stepIndex >= QUIZ_QUESTIONS.length) {
          const picks = await recommendTopFromQuiz(state.answers, 3);
          QUIZ_SESSIONS.delete(sessionKey);
          if (!picks.length) {
            const reply = "No sauna is available.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
            return res.json({ reply });
          }
          const lines = [];
          const n = picks.length;
          lines.push(n === 1 ? "Here is a great option based on your answers:" : `Here are ${n} great options based on your answers:`);
          for (const p of picks) {
            const price = p.price ? ` - Price from: ${p.price}` : "";
            lines.push(`- ${p.title}${price}`);
            if (p.url) lines.push(`  Click here - ${p.url}`);
          }
         
          const reply = lines.join("\n");
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
          return res.json({ reply });
        }
          const reply = [
            `Let's find your best match with a quick ${QUIZ_COUNT}-question quiz.`,
            formatQuizQuestion(state.stepIndex),
          ].join("\n\n");
        const quick = quickRepliesForStep(state.stepIndex);
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply, quick_replies: quick });
      }
      if (state) {
        if (state.customFirst && state.stepIndex === 0) {
          const mapped = parseCustomFirstAnswer(state.domain, userMsgRaw);
          if (!Object.keys(mapped).length) {
            const reply = [
              "Sorry, I didn't catch that.",
              formatCustomFirstQuestion(state.domain),
            ].join("\n\n");
            const quick = quickRepliesForCustom(state.domain);
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
            return res.json({ reply, quick_replies: quick });
          }
          Object.assign(state.answers, mapped);
          state.customFirst = false;
          while (state.stepIndex < QUIZ_QUESTIONS.length && state.answers[QUIZ_QUESTIONS[state.stepIndex].id]) {
            state.stepIndex++;
          }
          const reply = [
            `Got it. ${describeAnswers(state.answers)}`,
            formatQuizQuestion(state.stepIndex),
          ].join("\n\n");
          const quick = quickRepliesForStep(state.stepIndex);
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
          return res.json({ reply, quick_replies: quick });
        }
        const value = parseQuizAnswer(state.stepIndex, userMsgRaw);
        if (!value) {
          // If the user asked a non-quiz question, exit quiz and fall through to normal handling
          const looksLikeNonQuiz = SECTION_KEYWORDS_RX.test(userMsgRaw)
            || /https?:\/\//i.test(userMsgRaw)
            || /\b([A-Z]{1,4}-?\d{1,4}[A-Z0-9-]*)\b/i.test(userMsgRaw)
            || /\bcompare|vs\.?\b/i.test(userMsgRaw);
          if (looksLikeNonQuiz) {
            QUIZ_SESSIONS.delete(sessionKey);
          } else {
            const reply = [
              "Sorry, I didn't catch that.",
              formatQuizQuestion(state.stepIndex),
            ].join("\n\n");
            const quick = quickRepliesForStep(state.stepIndex);
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
            return res.json({ reply, quick_replies: quick });
          }
        }
        const q = QUIZ_QUESTIONS[state.stepIndex];
        state.answers[q.id] = value;
        state.stepIndex++;
        if (state.stepIndex < QUIZ_QUESTIONS.length) {
          const reply = [
            `Got it. ${describeAnswers(state.answers)}`,
            formatQuizQuestion(state.stepIndex),
          ].join("\n\n");
          const quick = quickRepliesForStep(state.stepIndex);
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
          return res.json({ reply, quick_replies: quick });
        }
        QUIZ_SESSIONS.delete(sessionKey);
        const picks = await recommendTopFromQuiz(state.answers, 3);
        if (!picks.length) {
          const reply = "No sauna is available.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
          return res.json({ reply });
        }
        const lines = [];
        const n = picks.length;
        lines.push(n === 1 ? "Here is a great option based on your answers:" : `Here are ${n} great options based on your answers:`);
        for (const p of picks) {
          const price = p.price ? ` - Price from: ${p.price}` : "";
          lines.push(`- ${p.title}${price}`);
          if (p.url) lines.push(`  Click here - ${p.url}`);
        }
        lines.push("\nIf you'd like, say 'start quiz' to refine further or ask me to compare any two options.");
        const reply = lines.join("\n");
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }
    }

    // MULTI-PRODUCT SPECS / DIMENSIONS HANDLER (e.g., "FD-6 and FD-7")
    {
      const wantsSpecs = /\b(specs?|specifications?)\b/i.test(userMsg);
      const wantsDims = /\b(dimension|dimensions|size|width|height|depth)\b/i.test(userMsg);
      const wantsIncluded = /\b(what'?s included|included|in the box)\b/i.test(userMsg);
      const wantsManuals = /\b(manuals?|installation|install|spec\s*sheet|wiring|pdfs?)\b/i.test(userMsg);
      if (wantsSpecs || wantsDims) {
        const modelTokens = Array.from(userMsg.matchAll(/\b([A-Z]{1,4}-?\d{1,4}[A-Z0-9-]*)\b/gi)).map(m => m[1]);
        const unique = Array.from(new Set(modelTokens.map(s => s.toLowerCase())));
        if (unique.length >= 2) {
          const results = [];
          for (const tok of unique.slice(0, 5)) {
            try {
              let h = await resolveRequestedProduct(tok);
              if (!h) h = await resolveRequestedProductLoose(tok);
              if (!h) continue;
              const p = await ensureProductByHandle(h);
              if (p) results.push(p);
            } catch {}
          }
          if (results.length) {
            const lines = [];
            for (const p of results) {
              const specs = slimSpecs(p.sections?.specifications || []);
              const dims = specs.filter(kv => /dimension|width|depth|height|size|interior|exterior/i.test(kv.key));
              lines.push(`${p.title}`);
              if (wantsSpecs) {
                lines.push(...(specs.slice(0, 20).map(kv => `${kv.key}: ${kv.value}`)));
              }
              if (wantsDims && (!wantsSpecs || dims.length)) {
                if (wantsSpecs) lines.push("");
                const d = dims.length ? dims : specs;
                lines.push(...(d.slice(0, 12).map(kv => `${kv.key}: ${kv.value}`)));
              }
              lines.push("");
            }
            const reply = lines.join("\n").trim();
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply });
            return res.json({ reply });
          }
        }
        // If we didn't find explicit tokens, try a loose single-product resolution for specs/dims
        let h2 = await resolveRequestedProduct(userMsg);
        if (!h2) h2 = await resolveRequestedProductLoose(userMsg);
        if (h2) {
          try {
            const p = await ensureProductByHandle(h2);
            if (p) {
              const specs = slimSpecs(p.sections?.specifications || []);
              const dims = specs.filter(kv => /dimension|width|depth|height|size|interior|exterior/i.test(kv.key));
              const lines = [];
              lines.push(`${p.title}`);
              if (wantsSpecs) lines.push(...(specs.slice(0, 20).map(kv => `${kv.key}: ${kv.value}`)));
              if (wantsDims) {
                const d = dims.length ? dims : specs;
                if (wantsSpecs) lines.push("");
                lines.push(...(d.slice(0, 12).map(kv => `${kv.key}: ${kv.value}`)));
              }
              const reply = lines.join("\n").trim();
              logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply });
              return res.json({ reply });
            }
          } catch {}
        }
      }

      // What's included / Manuals single-product quick answers
      if (wantsIncluded || wantsManuals) {
        let h3 = await resolveRequestedProduct(userMsg);
        if (!h3) h3 = await resolveRequestedProductLoose(userMsg);
        if (h3) {
          try {
            const p = await ensureProductByHandle(h3);
            if (p) {
              if (wantsIncluded) {
                const inc = (p.sections?.whats_included || []).slice(0, MAX.INCLUDED);
                const reply = inc.length ? inc.map((x) => `- ${x}`).join("\n") : "No included items listed.";
                logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: h3 });
                return res.json({ reply });
              }
              if (wantsManuals) {
                const links = extractManualLinks(p);
                const reply = links.length ? links.map(u => `Manual/Spec - ${u}`).join("\n") : "No manuals or installation PDFs found.";
                logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: h3 });
                return res.json({ reply });
              }
            }
          } catch {}
        }
      }
    }

    // EARLY: If the user provided a clear product link/name, answer immediately and skip quiz
    {
      // If the user refers to "this product" and we have a last product, use it directly
      const refersToCurrent = /\b(this|that|it|current|the product)\b/i.test(userMsg);
      if (refersToCurrent && LAST_PRODUCT) {
        const maybeHandle = LAST_PRODUCT.handle;
        const product = LAST_PRODUCT;
        // Intent detection for section-specific queries
        const wantsPriceOnly = /\b(price|cost|how much|what(?:'s| is) the price)\b/i.test(userMsg);
        const wantsFeaturesOnly = /\b(features?|highlights?)\b/i.test(userMsg);
        const wantsSpecsOnly = /\b(specs?|specifications?)\b/i.test(userMsg);
        const wantsSizeOnly = /\b(dimension|size|interior|exterior|width|depth|height|cubic\s*(feet|ft)|volume)\b/i.test(userMsg);
        const wantsIncludedOnly = /\b(what'?s included|in the box|included)\b/i.test(userMsg);
        const wantsWarrantyOnly = /\b(warranty|guarantee)\b/i.test(userMsg);
        const wantsShippingOnly = /\b(shipping|delivery|lead[-\s]?time)\b/i.test(userMsg);
        const wantsReturnsOnly = /\b(return|refund|exchange)\b/i.test(userMsg);
        const wantsProductInfoOnly = /\b(product information|product info|description|about( the)? product|details only)\b/i.test(userMsg);

        const askedForAll = /\b(all|everything|full (?:details?|dump|specs?)|show (?:all|everything)|product details?)\b/i.test(userMsg);
        const justProductName = !SECTION_KEYWORDS_RX.test(userMsgRaw);

        if (askedForAll) {
          const reply = formatProductFull(product) || "No data found for this product.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (justProductName) {
          const reply = formatProductOverview(product);
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsProductInfoOnly) {
          const reply = formatProductInfoOnly(product);
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsPriceOnly) {
          const price = product.price_from_formatted || usd(product.price_from);
          const out = [];
          if (price) out.push(`Price: ${price}`);
          out.push(`${product.title}`);
          if (product.url) out.push(`Click here - ${product.url}`);
          const reply = out.join("\n");
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsFeaturesOnly) {
          const feats = (product.sections?.features || []).slice(0, MAX.FEATURES);
          const reply = feats.length ? feats.map((x) => `- ${x}`).join("\n") : "No features available.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsSpecsOnly) {
          const specs = slimSpecs(product.sections?.specifications || []);
          const reply = specs.length ? specs.map((kv) => `${kv.key}: ${kv.value}`).join("\n") : "No specifications available.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsSizeOnly) {
          const specs = slimSpecs((product.sections?.specifications || []).filter((s) =>
            /dimension|interior|exterior|width|depth|height|volume|cubic\s*(feet|ft)/i.test(s.key)
          ));
          let lines = specs;
          if (!lines.length) {
            const rx = /\b(\d{2,3}(\.\d+)?)\s*(in|\")?\s*[x×]\s*(\d{2,3}(\.\d+)?)\s*(in|\")?\s*[x×]\s*(\d{2,3}(\.\d+)?)\b/i;
            const mined = (product.sections?.specifications || [])
              .filter(kv => rx.test(`${kv.key} ${kv.value}`))
              .map(kv => ({ key: kv.key, value: kv.value }));
            lines = slimSpecs(mined);
          }
          const reply = lines.length ? lines.map((kv) => `${kv.key}: ${kv.value}`).join("\n") : "No size information available.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsIncludedOnly) {
          const inc = (product.sections?.whats_included || []).slice(0, MAX.INCLUDED);
          const reply = inc.length ? inc.map((x) => `- ${x}`).join("\n") : "No included items listed.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsWarrantyOnly) {
          const arr = (product.sections?.warranty || []).slice(0, MAX.WARRANTY);
          const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No warranty info listed.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsShippingOnly) {
          const arr = (product.sections?.shipping || []).slice(0, MAX.SHIPPING);
          const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No shipping info listed.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }
        if (wantsReturnsOnly) {
          const arr = (product.sections?.returns || []).slice(0, MAX.RETURNS);
          const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No return policy info listed.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
          return res.json({ reply });
        }

        // General/fuzzy about this product → LLM
        const SYSTEM = sys(`You are SaunaBot 5000 for Peak Primal Wellness.
Only answer using SAUNA_DATA. Do not invent.
Prefer Product Information, Specifications, Features, What's Included, Warranty, Shipping, and Returns.
Do not include any Overview or FAQ text in your response.
Always include the product title and link when recommending.`);
        const SAUNA_DATA = slimCtxSingle(product, "general");
        let USER = `Question: ${userMsg}\n\nSAUNA_DATA:\n${SAUNA_DATA}\n\nRules:\n- Use the provided sections only.\n- Exclude Overview/FAQ.\n- Never show "$0".`;
        if (USER.length > MAX.PROMPT) USER = clip(USER, MAX.PROMPT);

        const { response: rBest, model: usedModel } = await chatWithFallback([
          { role: "system", content: SYSTEM },
          { role: "user", content: USER },
        ], { temperature: 0.2 });

        const rawReply = rBest.choices?.[0]?.message?.content ?? "";
        const reply = toPlainText(rawReply);
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle, model: usedModel });
        return res.json({ reply });
      }

      let maybeHandle = await resolveRequestedProduct(userMsg);
      if (!maybeHandle) maybeHandle = await resolveRequestedProductLoose(userMsg);
      if (maybeHandle) {
        const product = await ensureProductByHandle(maybeHandle);
        if (product) {
          LAST_PRODUCT = product;
          // If a quiz session existed, clear it to avoid conflicting flows
          QUIZ_SESSIONS.delete(getSessionKey(req));

          // Intent detection for section-specific queries
          const wantsPriceOnly = /\b(price|cost|how much|what(?:'s| is) the price)\b/i.test(userMsg);
          const wantsFeaturesOnly = /\b(features?|highlights?)\b/i.test(userMsg);
          const wantsSpecsOnly = /\b(specs?|specifications?)\b/i.test(userMsg);
          const wantsSizeOnly = /\b(dimension|size|interior|exterior|width|depth|height|cubic\s*(feet|ft)|volume)\b/i.test(userMsg);
          const wantsIncludedOnly = /\b(what'?s included|in the box|included)\b/i.test(userMsg);
          const wantsWarrantyOnly = /\b(warranty|guarantee)\b/i.test(userMsg);
          const wantsShippingOnly = /\b(shipping|delivery|lead[-\s]?time)\b/i.test(userMsg);
          const wantsReturnsOnly = /\b(return|refund|exchange)\b/i.test(userMsg);
          const wantsProductInfoOnly = /\b(product information|product info|description|about( the)? product|details only)\b/i.test(userMsg);

          const askedForAll = /\b(all|everything|full (?:details?|dump|specs?)|show (?:all|everything)|product details?)\b/i.test(userMsg);
          const justProductName = !SECTION_KEYWORDS_RX.test(userMsgRaw);

          if (askedForAll) {
            const reply = formatProductFull(product) || "No data found for this product.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (justProductName) {
            const reply = formatProductOverview(product);
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsProductInfoOnly) {
            const reply = formatProductInfoOnly(product);
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsPriceOnly) {
            const price = product.price_from_formatted || usd(product.price_from);
            const out = [];
            if (price) out.push(`Price: ${price}`);
            out.push(`${product.title}`);
            if (product.url) out.push(`Click here - ${product.url}`);
            const reply = out.join("\n");
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsFeaturesOnly) {
            const feats = (product.sections?.features || []).slice(0, MAX.FEATURES);
            const reply = feats.length ? feats.map((x) => `- ${x}`).join("\n") : "No features available.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsSpecsOnly) {
            const specs = slimSpecs(product.sections?.specifications || []);
            const reply = specs.length ? specs.map((kv) => `${kv.key}: ${kv.value}`).join("\n") : "No specifications available.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsSizeOnly) {
            const specs = slimSpecs((product.sections?.specifications || []).filter((s) =>
              /dimension|interior|exterior|width|depth|height|volume|cubic\s*(feet|ft)/i.test(s.key)
            ));
            let lines = specs;
            if (!lines.length) {
              const rx = /\b(\d{2,3}(\.\d+)?)\s*(in|\")?\s*[x×]\s*(\d{2,3}(\.\d+)?)\s*(in|\")?\s*[x×]\s*(\d{2,3}(\.\d+)?)\b/i;
              const mined = (product.sections?.specifications || [])
                .filter(kv => rx.test(`${kv.key} ${kv.value}`))
                .map(kv => ({ key: kv.key, value: kv.value }));
              lines = slimSpecs(mined);
            }
            const reply = lines.length ? lines.map((kv) => `${kv.key}: ${kv.value}`).join("\n") : "No size information available.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsIncludedOnly) {
            const inc = (product.sections?.whats_included || []).slice(0, MAX.INCLUDED);
            const reply = inc.length ? inc.map((x) => `- ${x}`).join("\n") : "No included items listed.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsWarrantyOnly) {
            const arr = (product.sections?.warranty || []).slice(0, MAX.WARRANTY);
            const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No warranty info listed.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsShippingOnly) {
            const arr = (product.sections?.shipping || []).slice(0, MAX.SHIPPING);
            const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No shipping info listed.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }
          if (wantsReturnsOnly) {
            const arr = (product.sections?.returns || []).slice(0, MAX.RETURNS);
            const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No return policy info listed.";
            logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle });
            return res.json({ reply });
          }

          // General/fuzzy about this product → LLM
          const SYSTEM = sys(`You are SaunaBot 5000 for Peak Primal Wellness.
Only answer using SAUNA_DATA. Do not invent.
Prefer Product Information, Specifications, Features, What's Included, Warranty, Shipping, and Returns.
Do not include any Overview or FAQ text in your response.
Always include the product title and link when recommending.`);
          const SAUNA_DATA = slimCtxSingle(product, "general");
          let USER = `Question: ${userMsg}\n\nSAUNA_DATA:\n${SAUNA_DATA}\n\nRules:\n- Use the provided sections only.\n- Exclude Overview/FAQ.\n- Never show "$0".`;
          if (USER.length > MAX.PROMPT) USER = clip(USER, MAX.PROMPT);

          const { response: rBest, model: usedModel } = await chatWithFallback([
            { role: "system", content: SYSTEM },
            { role: "user", content: USER },
          ], { temperature: 0.2 });

          const rawReply = rBest.choices?.[0]?.message?.content ?? "";
          const reply = toPlainText(rawReply);
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: maybeHandle, model: usedModel });
          return res.json({ reply });
        }
      }
    }

    // General recommendation (explicit list requests like "top 3", "show options")
    {
      const mentionsSauna = /\bsauna(s)?\b/i.test(userMsg);
      const explicitList = /\b(top\s*3|top\s*5|show|list|options?)\b/i.test(userMsg);
      if (mentionsSauna && explicitList) {
        const criteria = [];
        const queryKeywords = [];
        if (/\bindoor\b/i.test(userMsg)) { criteria.push("indoor"); queryKeywords.push("indoor sauna"); }
        if (/\boutdoor\b/i.test(userMsg)) { criteria.push("outdoor"); queryKeywords.push("outdoor sauna"); }
        const subtype = userMsg.match(/\b(infrared|traditional|barrel|steam|electric)\b/i);
        if (subtype) { criteria.push(subtype[1].toLowerCase()); queryKeywords.push(`${subtype[1]} sauna`); }

        const query = queryKeywords[0] || "sauna";
        const primaryHits = await predictiveSearch(query);
        let handles = (primaryHits || []).map((h) => h.handle);
        if (handles.length < 3) {
          const extra = await searchProductsByKeyword(query, 5, 10);
          for (const e of extra) if (!handles.includes(e.handle)) handles.push(e.handle);
        }
        if (handles.length < 3 && query !== "sauna") {
          const extra2 = await searchProductsByKeyword("sauna", 3, 10);
          for (const e of extra2) if (!handles.includes(e.handle)) handles.push(e.handle);
        }
        handles = Array.from(new Set(handles)).slice(0, 10);

        const products = [];
        for (const h of handles) {
          try {
            const p = await ensureProductByHandle(h);
            if (p && /sauna/i.test(p.title || "")) products.push(p);
          } catch {}
        }

        if (!products.length) {
          const reply = "No sauna is available.";
          logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
          return res.json({ reply });
        }

        const SYSTEM = sys(`You are SaunaBot 5000 for Peak Primal Wellness.
 Only answer using SAUNA_PRODUCTS. Do not invent.
 Prefer Product Information, Specifications, Features, What's Included, Warranty, Shipping, and Returns.
 Do not include any Overview or FAQ text in your response.
 Return plain text only.
 Task: From SAUNA_PRODUCTS, recommend the top 3 options and give brief reasons.`);

        let SAUNA_PRODUCTS = slimCtxMulti(products, "general");
        if (SAUNA_PRODUCTS.length > MAX.PROMPT) SAUNA_PRODUCTS = clip(SAUNA_PRODUCTS, MAX.PROMPT);

        const criteriaText = criteria.length ? `\nUser criteria to consider: ${criteria.join(", ")}.` : "";
        const USER = `Question: ${userMsg}\n\nSAUNA_PRODUCTS:\n${SAUNA_PRODUCTS}${criteriaText}\n\nInstructions:\n- Recommend 3 options.\n- For each, output: <Product Title> - <URL> and 2-3 short reasons.\n- Include price if available.\n- Exclude Overview/FAQ text.`;

        const { response, model } = await chatWithFallback([
          { role: "system", content: SYSTEM },
          { role: "user", content: USER },
        ], { temperature: 0.2 });

        const rawReply = response.choices?.[0]?.message?.content ?? "";
        const reply = toPlainText(rawReply);
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handles, model });
        return res.json({ reply });
      }
    }

    // New: handle best sauna type queries (indoor/outdoor/subtypes)
    const asksBest = /\b(best|top|which|recommend|choice|pick|prefer)\b/i.test(userMsg);
    const mentionsSauna = /\bsauna(s)?\b/i.test(userMsg);

    if (asksBest && mentionsSauna) {
      const criteria = [];
      const queryKeywords = [];
      if (/\bindoor\b/i.test(userMsg)) { criteria.push("indoor"); queryKeywords.push("indoor sauna"); }
      if (/\boutdoor\b/i.test(userMsg)) { criteria.push("outdoor"); queryKeywords.push("outdoor sauna"); }
      const subtype = userMsg.match(/\b(infrared|traditional|barrel|steam|electric)\b/i);
      if (subtype) { criteria.push(subtype[1].toLowerCase()); queryKeywords.push(`${subtype[1]} sauna`); }
      const capacity = userMsg.match(/\b([1-9])\s*(person|people|seater|seat)\b/i);
      if (capacity) criteria.push(`${capacity[1]} person`);

      const query = queryKeywords[0] || "sauna";

      // Gather candidate products
      const primaryHits = await predictiveSearch(query);
      let handles = (primaryHits || []).map((h) => h.handle);
      if (handles.length < 3) {
        const extra = await searchProductsByKeyword(query, 5, 8);
        for (const e of extra) if (!handles.includes(e.handle)) handles.push(e.handle);
      }
      // Fallback to generic if still too few
      if (handles.length < 3 && query !== "sauna") {
        const extra2 = await searchProductsByKeyword("sauna", 3, 8);
        for (const e of extra2) if (!handles.includes(e.handle)) handles.push(e.handle);
      }
      handles = Array.from(new Set(handles)).slice(0, 8);

      const products = [];
      for (const h of handles) {
        try {
          const p = await ensureProductByHandle(h);
          if (p && /sauna/i.test(p.title || "") ) products.push(p);
        } catch {}
      }

      if (!products.length) {
        const reply = "I couldn't find matching saunas on our site right now.";
        logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
        return res.json({ reply });
      }

      const SYSTEM = sys(`You are SaunaBot 5000 for Peak Primal Wellness.
Only answer using SAUNA_PRODUCTS. Do not invent.
Prefer Product Information, Specifications, Features, What's Included, Warranty, Shipping, and Returns.
Do not include any Overview or FAQ text in your response.
Return plain text only.
Task: From SAUNA_PRODUCTS, choose the single best sauna for a typical home installation and explain briefly why.`);

      let SAUNA_PRODUCTS = slimCtxMulti(products, "general");
      if (SAUNA_PRODUCTS.length > MAX.PROMPT) SAUNA_PRODUCTS = clip(SAUNA_PRODUCTS, MAX.PROMPT);

      const criteriaText = criteria.length ? `\nUser criteria to consider: ${criteria.join(", ")}.` : "";
      const USER = `Question: ${userMsg}\n\nSAUNA_PRODUCTS:\n${SAUNA_PRODUCTS}${criteriaText}\n\nInstructions:\n- Choose ONE product as the best.\n- Start with: Recommendation: <Product Title> - <URL>\n- Then list 3-5 short reasons (features, materials, size/capacity, warranty, value/price).\n- If price is available, include it.\n- Exclude Overview/FAQ text.`;

      const { response: rBest2, model: usedModel2 } = await chatWithFallback([
        { role: "system", content: SYSTEM },
        { role: "user", content: USER },
      ], { temperature: 0.2 });

      const rawReply = rBest2.choices?.[0]?.message?.content ?? "";
      const reply = toPlainText(rawReply);
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handles, model: usedModel2 });
      return res.json({ reply });
    }

    // Try resolve product from this message; otherwise use last one
    let handle = await resolveRequestedProduct(userMsg);
    if (!handle && LAST_PRODUCT) handle = LAST_PRODUCT.handle;
    // If still unresolved, try loose any-word resolver
    if (!handle) handle = await resolveRequestedProductLoose(userMsg);

    if (!handle) {
      // If identity was just completed very recently, greet instead of prompting for a product
      if (ident?.completedAt && Date.now() - ident.completedAt < 30000) {
        const reply = `Hello ${ident.name},  I'm here to help you today. What can I assist you with?`;
        logChatResponse({ ts: Date.now(), type: "chat", user: "[post-identity first message]", reply, handle: null });
        return res.json({ reply });
      }
      const reply = "Please paste the product link from our site (…/products/<handle>) or tell me the exact model name so I can fetch that specific product.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle: null });
      return res.json({ reply });
    }

    const product = await ensureProductByHandle(handle);
    if (!product) {
      const reply = "I couldn't load that product. Please double-check the link/handle and try again.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    LAST_PRODUCT = product;

    // Intent detection
    const wantsPriceOnly = /\b(price|cost|how much|what(?:'s| is) the price)\b/i.test(userMsg);
    const wantsFeaturesOnly = /\b(features?|highlights?)\b/i.test(userMsg);
    const wantsSpecsOnly = /\b(specs?|specifications?)\b/i.test(userMsg);
    const wantsSizeOnly = /\b(dimension|size|interior|exterior|width|depth|height|cubic\s*(feet|ft)|volume)\b/i.test(userMsg);
    const wantsIncludedOnly = /\b(what'?s included|in the box|included)\b/i.test(userMsg);
    const wantsWarrantyOnly = /\b(warranty|guarantee)\b/i.test(userMsg);
    const wantsShippingOnly = /\b(shipping|delivery|lead[-\s]?time)\b/i.test(userMsg);
    const wantsReturnsOnly = /\b(return|refund|exchange)\b/i.test(userMsg);
    const wantsProductInfoOnly = /\b(product information|product info|description|about( the)? product|details only)\b/i.test(userMsg);

    const askedForAll = /\b(all|everything|full (?:details?|dump|specs?)|show (?:all|everything)|product details?)\b/i.test(userMsg);
    const justProductName = !SECTION_KEYWORDS_RX.test(userMsgRaw);

    if (askedForAll) {
      const reply = formatProductFull(product) || "No data found for this product.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (justProductName) {
      const reply = formatProductOverview(product);
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }

    // SHORT-CIRCUIT ANSWERS
    if (wantsProductInfoOnly) {
      const reply = formatProductInfoOnly(product);
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsPriceOnly) {
      const price = product.price_from_formatted || usd(product.price_from);
      const out = [];
      if (price) out.push(`Price: ${price}`);
      out.push(`${product.title}`);
      if (product.url) out.push(`Click here - ${product.url}`);
      const reply = out.join("\n");
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsFeaturesOnly) {
      const feats = (product.sections?.features || []).slice(0, MAX.FEATURES);
      const reply = feats.length ? feats.map((x) => `- ${x}`).join("\n") : "No features available.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsSpecsOnly) {
      const specs = slimSpecs(product.sections?.specifications || []);
      const reply = specs.length ? specs.map((kv) => `${kv.key}: ${kv.value}`).join("\n") : "No specifications available.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsSizeOnly) {
      const specs = slimSpecs((product.sections?.specifications || []).filter((s) =>
        /dimension|interior|exterior|width|depth|height|volume|cubic\s*(feet|ft)/i.test(s.key)
      ));
      let lines = specs;
      if (!lines.length) {
        const rx = /\b(\d{2,3}(\.\d+)?)\s*(in|\")?\s*[x×]\s*(\d{2,3}(\.\d+)?)\s*(in|\")?\s*[x×]\s*(\d{2,3}(\.\d+)?)\b/i;
        const mined = (product.sections?.specifications || [])
          .filter(kv => rx.test(`${kv.key} ${kv.value}`))
          .map(kv => ({ key: kv.key, value: kv.value }));
        lines = slimSpecs(mined);
      }
      const reply = lines.length ? lines.map((kv) => `${kv.key}: ${kv.value}`).join("\n") : "No size information available.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsIncludedOnly) {
      const inc = (product.sections?.whats_included || []).slice(0, MAX.INCLUDED);
      const reply = inc.length ? inc.map((x) => `- ${x}`).join("\n") : "No included items listed.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsWarrantyOnly) {
      const arr = (product.sections?.warranty || []).slice(0, MAX.WARRANTY);
      const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No warranty info listed.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsShippingOnly) {
      const arr = (product.sections?.shipping || []).slice(0, MAX.SHIPPING);
      const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No shipping info listed.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }
    if (wantsReturnsOnly) {
      const arr = (product.sections?.returns || []).slice(0, MAX.RETURNS);
      const reply = arr.length ? arr.map((x) => `- ${x}`).join("\n") : "No return policy info listed.";
      logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle });
      return res.json({ reply });
    }

    // General/fuzzy → LLM (exclude overview/faq)
    const SYSTEM = sys(`You are SaunaBot 5000 for Peak Primal Wellness.
Only answer using SAUNA_DATA. Do not invent.
Prefer Product Information, Specifications, Features, What's Included, Warranty, Shipping, and Returns.
Do not include any Overview or FAQ text in your response.
Always include the product title and link when recommending.`);

    const SAUNA_DATA = slimCtxSingle(product, "general");
    let USER = `Question: ${userMsg}\n\nSAUNA_DATA:\n${SAUNA_DATA}\n\nRules:\n- Use the provided sections only.\n- Exclude Overview/FAQ.\n- Never show "$0".`;
    if (USER.length > MAX.PROMPT) USER = clip(USER, MAX.PROMPT);

    const { response: rBest3, model: usedModel3 } = await chatWithFallback([
      { role: "system", content: SYSTEM },
      { role: "user", content: USER },
    ], { temperature: 0.2 });

    const rawReply = rBest3.choices?.[0]?.message?.content ?? "";
    const reply = toPlainText(rawReply);
    logChatResponse({ ts: Date.now(), type: "chat", user: userMsgRaw, reply, handle, model: usedModel3 });
    res.json({ reply });
  } catch (e) {
    console.error("CHAT ERR", e);
    logChatResponse({ ts: Date.now(), type: "chat", user: req.body?.message || "", reply: null, error: e?.error?.message || e.message });
    res.status(500).json({ error: e?.error?.message || e.message });
  }
});

/* ====================== STARTUP ====================== */
// Only start a local HTTP server when not running on Vercel. On Vercel, the app
// is exported and wrapped by a serverless function in /api.
if (!process.env.VERCEL) {
  app.listen(PORT, async () => {
    console.log(`🚀 SaunaBot running on http://localhost:${PORT} (bulk ingest, no overview/faq; specs fixed)`);
    console.log(`🗂️  Use /ingest/all to batch ingest; /ingest/status to monitor; /ingest/cancel to stop.`);
  });
}

export default app;

function cleanupUserSessions() {
  const now = Date.now();
  for (const [k, v] of USER_SESSIONS.entries()) {
    if (!v?.createdAt || now - v.createdAt > USER_TTL_MS) USER_SESSIONS.delete(k);
  }
}
function getOrCreateUserSession(sessionKey) {
  let s = USER_SESSIONS.get(sessionKey);
  if (!s) { s = { name: null, email: null, createdAt: Date.now() }; USER_SESSIONS.set(sessionKey, s); }
  return s;
}
function extractEmail(text = "") {
  const m = String(text).match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i);
  return m ? m[0].toLowerCase() : null;
}
function extractName(text = "") {
  const s = String(text).trim();
  // Labelled forms: Name: John Doe, Full Name: John Doe
  const mLabel = s.match(/\b(?:full\s*name|name)\s*[:=]\s*([A-Za-z][A-Za-z\-'. ]{1,80})\b/i);
  if (mLabel) return mLabel[1].trim();
  // Natural phrases
  const m1 = s.match(/\b(?:my name is|i am|i'm|this is)\s+([A-Za-z][A-Za-z\-'. ]{1,80})\b/i);
  if (m1) return m1[1].trim();
  // If the whole message is a short name-like token
  if (/^[A-Za-z][A-Za-z\-'. ]{1,80}$/.test(s)) return s.trim();
  return null;
}
function ensureUserIdentity(sessionKey, text) {
  const sess = getOrCreateUserSession(sessionKey);
  const beforeComplete = !!(sess.name && sess.email);
  const containsIdentityText = /\b(full\s*name|name)\s*[:=]|\b(my name is|i am|i'm|this is)\b|\bemail\b\s*[:=]|@[A-Z0-9._%+-]+/i.test(String(text || ""));
  if (!sess.email) {
    const e = extractEmail(text);
    if (e) sess.email = e;
  }
  if (!sess.name) {
    const n = extractName(text);
    if (n) sess.name = n;
  }
  const afterComplete = !!(sess.name && sess.email);
  // Treat any explicit identity message as a (re)submission and greet
  if (afterComplete && (containsIdentityText || !beforeComplete)) {
    sess.completedAt = Date.now();
    return { ok: false, reply: `Hello ${sess.name},  I'm here to help you today. What can I assist you with?`, justCompleted: true, name: sess.name, email: sess.email };
  }
  if (!afterComplete) {
    if (!sess.name && !sess.email) {
      return { ok: false, reply: "Before we begin, please share your name and email. Example: Name: John Doe, Email: john@example.com" };
    }
    if (!sess.name) {
      return { ok: false, reply: "Please share your name to continue. Example: Name: John Doe" };
    }
    if (!sess.email) {
      return { ok: false, reply: "Please share your email to continue. Example: Email: john@example.com" };
    }
  }
  return { ok: true, name: sess.name, email: sess.email, completedAt: sess.completedAt };
}
