/**
 * Audience Pulse — weekly Reddit + Bluesky scraping + Claude Haiku summarization
 *
 * Generates a once-per-week sentiment summary across utility-relevant themes.
 * Designed for cost: only one model call per theme per week (~$0.02/theme).
 *
 * Cache file: pulse-cache.json on disk so summaries survive Render restarts.
 */
const fs = require('fs');
const path = require('path');

const CACHE_FILE = path.join(__dirname, 'pulse-cache.json');
const HAIKU_MODEL = 'claude-haiku-4-5-20251001';
const REDDIT_UA = 'MGE-Social-Command-Center/1.0 (by u/taylormcgraham; Audience Pulse theme monitoring)';

// ============================================================
// Theme config — keywords + recommended subreddits per theme.
// Reddit search runs across all of Reddit (broad signal) plus the
// listed subreddits (focused signal). Bluesky just uses keywords.
// ============================================================
const PULSE_THEMES = [
  {
    id: 'energy_conservation',
    label: 'Energy Conservation',
    color: '#16a34a',
    keywords: ['"energy conservation"', '"saving energy"', '"lower electric bill"', '"reduce energy use"'],
    subreddits: ['Frugal', 'HomeImprovement', 'energy']
  },
  {
    id: 'affordability',
    label: 'Affordability',
    color: '#f97316',
    keywords: ['"utility bill" expensive', '"electric bill" high', '"energy cost" affordability', '"cant afford" electric'],
    subreddits: ['personalfinance', 'Frugal', 'povertyfinance']
  },
  {
    id: 'electric_vehicles',
    label: 'Electric Vehicles',
    color: '#2563eb',
    keywords: ['"EV charging" home', '"electric vehicle" charger', '"Level 2 charger"', '"home EV"'],
    subreddits: ['electricvehicles', 'TeslaModelY', 'ElectricCars']
  },
  {
    id: 'data_centers',
    label: 'Data Centers',
    color: '#7c3aed',
    keywords: ['"data center" power', '"data center" electricity', '"data center" utility', '"AI data center" energy'],
    subreddits: ['datacenter', 'sysadmin', 'energy']
  },
  {
    id: 'renewable_energy',
    label: 'Renewable Energy',
    color: '#10b981',
    keywords: ['"renewable energy"', '"clean energy"', '"wind energy"', '"green power"'],
    subreddits: ['RenewableEnergy', 'energy', 'climate']
  },
  {
    id: 'reliability',
    label: 'Reliability',
    color: '#dc2626',
    keywords: ['"power outage"', '"grid reliability"', '"blackout" utility', '"power restored"'],
    subreddits: ['preppers', 'energy', 'wisconsin']
  },
  {
    id: 'electrification',
    label: 'Electrification',
    color: '#0ea5e9',
    keywords: ['"heat pump" install', '"induction stove"', '"electric water heater"', 'electrification home'],
    subreddits: ['heatpumps', 'HomeImprovement', 'hvacadvice']
  },
  {
    id: 'rooftop_solar',
    label: 'Rooftop Solar & Net Metering',
    color: '#facc15',
    keywords: ['"rooftop solar"', '"net metering"', '"solar panels" home', '"residential solar"'],
    subreddits: ['solar', 'RenewableEnergy', 'energy']
  },
  {
    id: 'tou_rates',
    label: 'Time-of-Use Rates & Rate Cases',
    color: '#a855f7',
    keywords: ['"time of use" rate', '"TOU rate"', '"rate case" utility', '"rate hike" electric', '"rate increase" utility'],
    subreddits: ['energy', 'electricvehicles', 'personalfinance']
  },
  {
    id: 'winter_heating',
    label: 'Winter Heating Costs',
    color: '#3b82f6',
    keywords: ['"winter heating bill"', '"natural gas heating"', '"furnace cost"', '"heating bill" expensive'],
    subreddits: ['Frugal', 'wisconsin', 'minnesota', 'HomeImprovement']
  },
  {
    id: 'gas_bans',
    label: 'Natural Gas & Gas Bans',
    color: '#ea580c',
    keywords: ['"natural gas ban"', '"gas stove ban"', '"gas hookup" ban', '"induction vs gas"'],
    subreddits: ['energy', 'climate']
  }
];

// ============================================================
// ISO week ID — used to detect when a new week starts
// Format: YYYY-Www (e.g., 2026-W18)
// ============================================================
function getIsoWeekId(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNum = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return d.getUTCFullYear() + '-W' + String(weekNum).padStart(2, '0');
}

// ============================================================
// Cache I/O
// ============================================================
function loadCache() {
  try {
    if (!fs.existsSync(CACHE_FILE)) return null;
    const raw = fs.readFileSync(CACHE_FILE, 'utf8');
    return JSON.parse(raw);
  } catch (e) {
    console.warn(' [PULSE] Failed to load cache:', e.message);
    return null;
  }
}

function saveCache(cache) {
  try {
    fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), 'utf8');
  } catch (e) {
    console.warn(' [PULSE] Failed to save cache:', e.message);
  }
}

// ============================================================
// Reddit search — broad + per-subreddit
// ============================================================
async function fetchRedditForTheme(theme) {
  const items = [];
  const seen = new Set();
  // Broad search (all of Reddit) — one query combining all keywords with OR
  const broadQuery = theme.keywords.join(' OR ');
  const broadUrl = 'https://www.reddit.com/search.json?q=' +
                   encodeURIComponent(broadQuery) + '&sort=relevance&t=month&limit=25';
  // Per-subreddit search — joined subreddits, sorted by new
  const subredditList = theme.subreddits.join('+');
  const focusedUrl = 'https://www.reddit.com/r/' + subredditList +
                     '/search.json?q=' + encodeURIComponent(broadQuery) +
                     '&restrict_sr=1&sort=new&t=month&limit=15';

  for (const url of [broadUrl, focusedUrl]) {
    try {
      const resp = await fetch(url, {
        headers: { 'User-Agent': REDDIT_UA, 'Accept': 'application/json' }
      });
      if (!resp.ok) {
        console.warn(' [PULSE] Reddit ' + resp.status + ' for ' + theme.id);
        continue;
      }
      const data = await resp.json();
      const children = (data && data.data && data.data.children) || [];
      for (const c of children) {
        const d = c.data;
        if (!d || !d.id || seen.has(d.id)) continue;
        if ((d.score || 0) < 2) continue; // filter low-quality
        seen.add(d.id);
        items.push({
          source: 'reddit',
          id: d.id,
          title: d.title || '',
          body: (d.selftext || '').slice(0, 400),
          subreddit: d.subreddit || '',
          author: d.author || '',
          score: d.score || 0,
          comments: d.num_comments || 0,
          createdAt: new Date((d.created_utc || 0) * 1000).toISOString(),
          url: 'https://www.reddit.com' + (d.permalink || '')
        });
      }
    } catch (e) {
      console.warn(' [PULSE] Reddit fetch error for ' + theme.id + ':', e.message);
    }
  }
  // Cap at 30, sorted by engagement (score + comments)
  items.sort((a, b) => (b.score + b.comments) - (a.score + a.comments));
  return items.slice(0, 30);
}

// ============================================================
// Bluesky search — public, no auth
// ============================================================
async function fetchBlueskyForTheme(theme) {
  const items = [];
  const seen = new Set();
  // Use first 2 keywords to limit calls
  for (const kw of theme.keywords.slice(0, 2)) {
    try {
      const url = 'https://public.api.bsky.app/xrpc/app.bsky.feed.searchPosts?q=' +
                  encodeURIComponent(kw) + '&limit=15&sort=latest';
      const resp = await fetch(url, { headers: { 'Accept': 'application/json' } });
      if (!resp.ok) continue;
      const data = await resp.json();
      const posts = data.posts || [];
      for (const p of posts) {
        if (!p || !p.uri || seen.has(p.uri)) continue;
        const text = (p.record && p.record.text) || '';
        if (!text || text.length < 20) continue;
        seen.add(p.uri);
        // Convert at:// URI to web URL: https://bsky.app/profile/<handle>/post/<rkey>
        const handle = p.author && p.author.handle;
        const rkey = p.uri.split('/').pop();
        const webUrl = handle && rkey
          ? 'https://bsky.app/profile/' + handle + '/post/' + rkey
          : '';
        items.push({
          source: 'bluesky',
          id: p.uri,
          title: text.slice(0, 120),
          body: text.slice(0, 400),
          author: handle || '',
          score: (p.likeCount || 0) + (p.repostCount || 0),
          comments: p.replyCount || 0,
          createdAt: (p.record && p.record.createdAt) || new Date().toISOString(),
          url: webUrl
        });
      }
    } catch (e) {
      console.warn(' [PULSE] Bluesky fetch error for ' + theme.id + ':', e.message);
    }
  }
  items.sort((a, b) => b.score - a.score);
  return items.slice(0, 15);
}

// ============================================================
// Summarization — one call per theme
// Dual-provider: prefer Gemini (free tier), fall back to Claude Haiku.
// ============================================================
const GEMINI_MODEL = process.env.PULSE_GEMINI_MODEL || 'gemini-2.0-flash';
const PULSE_SYSTEM_PROMPT =
  'You are an audience-research analyst at Madison Gas and Electric (MGE), a Wisconsin utility. ' +
  'You read recent public commentary from Reddit and Bluesky and produce a tight, neutral, ' +
  'editorial-quality summary for the marketing and communications team. Hard rules: ' +
  '(1) Ground every claim in the actual posts provided — do not invent quotes or stats. ' +
  '(2) Sentiment is one of: positive, mixed, negative, or low_signal (use low_signal if there are fewer than 5 substantive posts). ' +
  '(3) Themes should be 3-5 short noun-phrases describing what people are actually talking about. ' +
  '(4) Summary is 2-3 sentences, plain professional tone, no marketing buzzwords. ' +
  '(5) Output strict JSON only — no preamble, no markdown.';

function buildThemeUserPrompt(theme, posts) {
  const trimmed = posts.slice(0, 30).map((p, i) => {
    const meta = p.source === 'reddit'
      ? '[r/' + p.subreddit + ', score ' + p.score + ', ' + p.comments + ' comments]'
      : '[Bluesky, ' + p.score + ' likes, ' + p.comments + ' replies]';
    return (i + 1) + '. ' + meta + ' "' + p.title + '"' +
           (p.body && p.body.length > 20 ? '\n   Body: ' + p.body.replace(/\s+/g, ' ').slice(0, 250) : '');
  }).join('\n\n');

  return 'Theme: ' + theme.label + '\n\n' +
         'Posts (Reddit + Bluesky, last ~30 days):\n\n' + trimmed + '\n\n' +
         'Return strict JSON in exactly this shape:\n' +
         '{\n' +
         '  "sentiment": "positive" | "mixed" | "negative" | "low_signal",\n' +
         '  "summary": "2-3 sentence editorial summary",\n' +
         '  "themes": ["short phrase", "short phrase", ...],\n' +
         '  "notable_quote": "one short representative quote from the posts (or empty string if none stands out)"\n' +
         '}';
}

function parseModelOutput(text) {
  const cleaned = (text || '').replace(/^```(?:json)?\s*/i, '').replace(/\s*```\s*$/i, '').trim();
  try {
    const parsed = JSON.parse(cleaned);
    return {
      sentiment: ['positive', 'mixed', 'negative', 'low_signal'].includes(parsed.sentiment) ? parsed.sentiment : 'mixed',
      summary: String(parsed.summary || '').slice(0, 800),
      themes: Array.isArray(parsed.themes) ? parsed.themes.map(String).slice(0, 5) : [],
      notable_quote: String(parsed.notable_quote || '').slice(0, 280)
    };
  } catch (e) {
    return { sentiment: 'mixed', summary: cleaned.slice(0, 400), themes: [], notable_quote: '' };
  }
}

async function callGemini(apiKey, userPrompt) {
  const url = 'https://generativelanguage.googleapis.com/v1beta/models/' +
              encodeURIComponent(GEMINI_MODEL) + ':generateContent?key=' + encodeURIComponent(apiKey);
  const resp = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      systemInstruction: { parts: [{ text: PULSE_SYSTEM_PROMPT }] },
      contents: [{ role: 'user', parts: [{ text: userPrompt }] }],
      generationConfig: {
        temperature: 0.4,
        maxOutputTokens: 800,
        responseMimeType: 'application/json'
      }
    })
  });
  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error('Gemini ' + resp.status + ': ' + errText.slice(0, 200));
  }
  const payload = await resp.json();
  const parts = payload.candidates && payload.candidates[0] &&
                payload.candidates[0].content && payload.candidates[0].content.parts;
  return (parts && parts[0] && parts[0].text) || '';
}

async function callClaude(apiKey, userPrompt) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01'
    },
    body: JSON.stringify({
      model: HAIKU_MODEL,
      max_tokens: 600,
      system: PULSE_SYSTEM_PROMPT,
      messages: [{ role: 'user', content: userPrompt }]
    })
  });
  if (!resp.ok) {
    const errText = await resp.text();
    throw new Error('Claude ' + resp.status + ': ' + errText.slice(0, 200));
  }
  const payload = await resp.json();
  return (payload.content && payload.content[0] && payload.content[0].text) || '';
}

async function summarizeTheme(theme, posts) {
  const geminiKey = process.env.GEMINI_API_KEY;
  const anthropicKey = process.env.ANTHROPIC_API_KEY;
  if (!geminiKey && !anthropicKey) {
    return {
      sentiment: 'low_signal',
      summary: 'No LLM API key configured (GEMINI_API_KEY or ANTHROPIC_API_KEY).',
      themes: [],
      notable_quote: ''
    };
  }
  if (posts.length === 0) {
    return {
      sentiment: 'low_signal',
      summary: 'No relevant posts found this week.',
      themes: [],
      notable_quote: ''
    };
  }
  const userPrompt = buildThemeUserPrompt(theme, posts);
  // Prefer Gemini (free tier); fall back to Claude on error or if Gemini key missing.
  if (geminiKey) {
    try {
      const text = await callGemini(geminiKey, userPrompt);
      return parseModelOutput(text);
    } catch (e) {
      console.warn(' [PULSE] Gemini failed for ' + theme.id + ': ' + e.message);
      if (!anthropicKey) {
        return { sentiment: 'low_signal', summary: 'Gemini call failed: ' + e.message, themes: [], notable_quote: '' };
      }
      // fall through to Claude
    }
  }
  try {
    const text = await callClaude(anthropicKey, userPrompt);
    return parseModelOutput(text);
  } catch (e) {
    console.warn(' [PULSE] Claude fallback failed for ' + theme.id + ': ' + e.message);
    return { sentiment: 'low_signal', summary: 'Summary generation failed.', themes: [], notable_quote: '' };
  }
}

// ============================================================
// Generate full pulse — one theme at a time, with brief pauses
// to avoid hammering Reddit
// ============================================================
async function generatePulse() {
  console.log(' [PULSE] Starting generation for week ' + getIsoWeekId(new Date()) + '...');
  const startedAt = Date.now();
  const themes = [];

  for (const theme of PULSE_THEMES) {
    try {
      const [redditPosts, blueskyPosts] = await Promise.all([
        fetchRedditForTheme(theme),
        fetchBlueskyForTheme(theme)
      ]);
      const allPosts = [...redditPosts, ...blueskyPosts];
      const ai = await summarizeTheme(theme, allPosts);
      // Top sources: 5 highest-engagement posts
      const sources = [...allPosts]
        .sort((a, b) => (b.score + b.comments) - (a.score + a.comments))
        .slice(0, 8)
        .map(p => ({
          source: p.source,
          title: p.title,
          subreddit: p.subreddit || '',
          author: p.author,
          score: p.score,
          comments: p.comments,
          url: p.url,
          createdAt: p.createdAt
        }));
      themes.push({
        id: theme.id,
        label: theme.label,
        color: theme.color,
        postCount: allPosts.length,
        redditCount: redditPosts.length,
        blueskyCount: blueskyPosts.length,
        sentiment: ai.sentiment,
        summary: ai.summary,
        keyThemes: ai.themes,
        notableQuote: ai.notable_quote,
        sources: sources
      });
      console.log(' [PULSE]   ' + theme.label + ': ' + allPosts.length + ' posts, sentiment=' + ai.sentiment);
      // Polite delay between themes (Reddit asks for ~1 req/sec)
      await new Promise(r => setTimeout(r, 1500));
    } catch (e) {
      console.warn(' [PULSE] Theme ' + theme.id + ' failed:', e.message);
      themes.push({
        id: theme.id,
        label: theme.label,
        color: theme.color,
        postCount: 0,
        sentiment: 'low_signal',
        summary: 'Generation failed: ' + e.message,
        keyThemes: [],
        notableQuote: '',
        sources: []
      });
    }
  }

  const cache = {
    weekId: getIsoWeekId(new Date()),
    generatedAt: new Date().toISOString(),
    durationMs: Date.now() - startedAt,
    themes: themes
  };
  saveCache(cache);
  console.log(' [PULSE] Generation complete in ' + Math.round(cache.durationMs / 1000) + 's');
  return cache;
}

// ============================================================
// Public API
// ============================================================
let _generationInFlight = null;

async function getPulseData(forceRefresh) {
  const cache = loadCache();
  const currentWeek = getIsoWeekId(new Date());

  // Cache is fresh — return it
  if (!forceRefresh && cache && cache.weekId === currentWeek) {
    return cache;
  }

  // Already generating — wait on the in-flight promise
  if (_generationInFlight) {
    return _generationInFlight;
  }

  // Need to regenerate
  _generationInFlight = generatePulse()
    .catch(err => {
      console.warn(' [PULSE] Generation failed:', err.message);
      // Fall back to stale cache rather than nothing
      return cache || { weekId: currentWeek, generatedAt: null, themes: [], error: err.message };
    })
    .finally(() => {
      _generationInFlight = null;
    });

  return _generationInFlight;
}

// Returns cache only without triggering generation — for the no-wait path
function getCachedPulse() {
  return loadCache();
}

// Trigger initial generation if cache is empty or stale.
// Called on server start, fires-and-forgets (don't block startup).
function maybeBackgroundGenerate() {
  const cache = loadCache();
  const currentWeek = getIsoWeekId(new Date());
  if (cache && cache.weekId === currentWeek) {
    console.log(' [PULSE] Cache fresh for week ' + currentWeek + ', no generation needed');
    return;
  }
  if (!process.env.GEMINI_API_KEY && !process.env.ANTHROPIC_API_KEY) {
    console.log(' [PULSE] Skipping background generation — no LLM key (set GEMINI_API_KEY or ANTHROPIC_API_KEY)');
    return;
  }
  // Delay 30s so server is fully up before we start hammering Reddit
  setTimeout(() => {
    console.log(' [PULSE] Triggering background generation (cache is ' +
                (cache ? 'stale, week=' + cache.weekId : 'empty') + ')');
    getPulseData().catch(err => console.warn(' [PULSE] Background generation failed:', err.message));
  }, 30000);
}

module.exports = {
  PULSE_THEMES,
  getPulseData,
  getCachedPulse,
  maybeBackgroundGenerate,
  getIsoWeekId
};
