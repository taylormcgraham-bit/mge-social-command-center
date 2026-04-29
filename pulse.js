/**
 * Audience Pulse — weekly Reddit + Bluesky scraping with comment-level summarization.
 *
 * Architecture:
 *  1. For each theme: ONE combined-OR Reddit search across curated energy/utility subreddits.
 *     If month-window returns <5 posts, retry with year window for low-volume themes.
 *  2. No strict phrase filter — subreddits are pre-curated for relevance.
 *  3. For top 8 highest-engagement posts, fetch top 5 user comments (the actual user voice).
 *  4. Send post titles + bodies + top comments to Claude Haiku 4.5 (with Gemini fallback).
 *  5. Reddit calls retry on transient errors (429/503) with exponential backoff.
 *  6. topComments are preserved on each source so the UI can render the actual user voice.
 *  7. Cache file: pulse-cache.json on disk, regenerated once per ISO week.
 */
const fs = require('fs');
const path = require('path');

const CACHE_FILE = path.join(__dirname, 'pulse-cache.json');
const HAIKU_MODEL = process.env.PULSE_CLAUDE_MODEL || 'claude-haiku-4-5-20251001';
const GEMINI_MODEL = process.env.PULSE_GEMINI_MODEL || 'gemini-2.0-flash';
const REDDIT_UA = 'MGE-Social-Command-Center/1.0 (by u/taylormcgraham; Audience Pulse theme monitoring)';

// Rate limiting / retry
const REDDIT_DELAY_MS = 3000;       // bumped from 1.5s — Reddit 429s consistently around theme 7 with 1.5s
const CLAUDE_DELAY_MS = 3000;       // ~20 RPM, well under tier-1 50 RPM limit
const MAX_RETRIES = 4;              // bumped from 3 — gives us 3+6+12+24=45s of total backoff per call

// ============================================================
// Theme config — curated subreddits per theme.
// ============================================================
const PULSE_THEMES = [
  {
    id: 'energy_conservation',
    label: 'Energy Conservation',
    color: '#16a34a',
    keywords: ['energy conservation', 'saving energy', 'lower electric bill', 'reduce energy use', 'energy efficiency'],
    subreddits: ['energy', 'Frugal', 'HomeImprovement', 'BuyItForLife', 'Conservation']
  },
  {
    id: 'affordability',
    label: 'Affordability',
    color: '#f97316',
    keywords: ['utility bill', 'electric bill', 'energy bill', 'rate hike', 'rate increase', 'cant afford electric', 'high energy cost'],
    subreddits: ['povertyfinance', 'personalfinance', 'Frugal', 'energy', 'middleclassfinance']
  },
  {
    id: 'electric_vehicles',
    label: 'Electric Vehicles',
    color: '#2563eb',
    keywords: ['EV charging', 'electric vehicle', 'EV charger', 'home charger', 'level 2 charger', 'charging station'],
    subreddits: ['electricvehicles', 'TeslaModelY', 'TeslaModel3', 'Bolt_EV', 'Mach_E', 'evcharging', 'F150Lightning']
  },
  {
    id: 'data_centers',
    label: 'Data Centers',
    color: '#7c3aed',
    keywords: ['data center', 'data centers', 'AI data center', 'hyperscaler', 'data center power'],
    subreddits: ['energy', 'datacenter', 'sysadmin', 'wisconsin', 'technology']
  },
  {
    id: 'renewable_energy',
    label: 'Renewable Energy',
    color: '#10b981',
    keywords: ['renewable energy', 'clean energy', 'wind energy', 'wind power', 'green energy', 'wind farm', 'solar farm'],
    subreddits: ['RenewableEnergy', 'energy', 'climate', 'climatechange', 'sustainability']
  },
  {
    id: 'electrification',
    label: 'Electrification',
    color: '#0ea5e9',
    keywords: ['heat pump', 'induction stove', 'electric water heater', 'electrification', 'all electric home', 'electric heating'],
    subreddits: ['heatpumps', 'HomeImprovement', 'hvacadvice', 'AskElectricians', 'energy']
  },
  {
    id: 'rooftop_solar',
    label: 'Rooftop Solar & Net Metering',
    color: '#facc15',
    keywords: ['rooftop solar', 'net metering', 'solar panels', 'residential solar', 'home solar', 'solar install'],
    subreddits: ['solar', 'SolarDIY', 'RenewableEnergy', 'energy']
  },
  {
    id: 'winter_heating',
    label: 'Winter Heating Costs',
    color: '#3b82f6',
    keywords: ['heating bill', 'winter heating', 'natural gas heating', 'furnace cost', 'gas bill winter', 'cold weather bill'],
    subreddits: ['Frugal', 'wisconsin', 'minnesota', 'HomeImprovement', 'hvacadvice', 'heatpumps']
  },
];

// ============================================================
// Utilities
// ============================================================
function getIsoWeekId(date) {
  const d = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()));
  const dayNum = d.getUTCDay() || 7;
  d.setUTCDate(d.getUTCDate() + 4 - dayNum);
  const yearStart = new Date(Date.UTC(d.getUTCFullYear(), 0, 1));
  const weekNum = Math.ceil((((d - yearStart) / 86400000) + 1) / 7);
  return d.getUTCFullYear() + '-W' + String(weekNum).padStart(2, '0');
}
function delay(ms) { return new Promise(r => setTimeout(r, ms)); }
function loadCache() {
  try {
    if (!fs.existsSync(CACHE_FILE)) return null;
    return JSON.parse(fs.readFileSync(CACHE_FILE, 'utf8'));
  } catch (e) { console.warn(' [PULSE] Failed to load cache:', e.message); return null; }
}
function saveCache(cache) {
  try { fs.writeFileSync(CACHE_FILE, JSON.stringify(cache, null, 2), 'utf8'); }
  catch (e) { console.warn(' [PULSE] Failed to save cache:', e.message); }
}

// ============================================================
// Reddit fetch with retry
// ============================================================
async function fetchRedditWithRetry(url, label) {
  let lastErr;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try {
      const resp = await fetch(url, {
        headers: { 'User-Agent': REDDIT_UA, 'Accept': 'application/json' }
      });
      if (resp.status === 429 || resp.status === 503) {
        const wait = Math.pow(2, attempt) * 3000;
        console.warn(' [PULSE] Reddit ' + resp.status + ' for ' + label + ', waiting ' + wait + 'ms');
        await delay(wait);
        continue;
      }
      if (!resp.ok) {
        console.warn(' [PULSE] Reddit ' + resp.status + ' for ' + label);
        return null;
      }
      return await resp.json();
    } catch (e) {
      lastErr = e;
      if (attempt === MAX_RETRIES - 1) { console.warn(' [PULSE] Reddit error for ' + label + ':', e.message); return null; }
      await delay(Math.pow(2, attempt) * 2000);
    }
  }
  return null;
}

// ============================================================
// Reddit posts — single combined OR search per theme
// (no strict phrase filter; the curated subreddit list is the relevance gate)
// ============================================================
async function searchRedditPosts(theme, timeWindow) {
  const subList = theme.subreddits.join('+');
  // Combine all keywords with OR. Phrase-quote each one.
  const q = theme.keywords.map(k => '"' + k + '"').join(' OR ');
  const url = 'https://www.reddit.com/r/' + subList + '/search.json' +
              '?q=' + encodeURIComponent(q) +
              '&restrict_sr=1&sort=relevance&t=' + timeWindow + '&limit=25';
  const data = await fetchRedditWithRetry(url, theme.id + ' (' + timeWindow + ')');
  if (!data) return [];
  const children = (data && data.data && data.data.children) || [];
  const items = [];
  for (const c of children) {
    const d = c.data;
    if (!d || !d.id) continue;
    if ((d.score || 0) < 2) continue;
    items.push({
      source: 'reddit',
      id: d.id,
      title: d.title || '',
      selftext: (d.selftext || '').slice(0, 600),
      subreddit: d.subreddit || '',
      author: d.author || '',
      score: d.score || 0,
      comments: d.num_comments || 0,
      createdAt: new Date((d.created_utc || 0) * 1000).toISOString(),
      permalink: d.permalink || '',
      url: 'https://www.reddit.com' + (d.permalink || '')
    });
  }
  return items;
}

async function fetchRedditPostsForTheme(theme) {
  // Try month first
  let posts = await searchRedditPosts(theme, 'month');
  await delay(REDDIT_DELAY_MS);
  // If too few results, widen to year (low-volume themes like rate cases)
  if (posts.length < 5) {
    const yearPosts = await searchRedditPosts(theme, 'year');
    await delay(REDDIT_DELAY_MS);
    const seen = new Set(posts.map(p => p.id));
    for (const p of yearPosts) if (!seen.has(p.id)) posts.push(p);
  }
  posts.sort((a, b) => (b.score + b.comments) - (a.score + a.comments));
  return posts.slice(0, 25);
}

// Top comments on a single post
async function fetchTopCommentsForPost(post, limit) {
  if (!post.permalink) return [];
  const url = 'https://www.reddit.com' + post.permalink + '.json?limit=20&sort=top&depth=1';
  const data = await fetchRedditWithRetry(url, 'comments-' + post.id);
  if (!Array.isArray(data) || data.length < 2) return [];
  const commentChildren = (data[1] && data[1].data && data[1].data.children) || [];
  const comments = [];
  for (const c of commentChildren) {
    if (!c || c.kind !== 't1' || !c.data) continue;
    const cd = c.data;
    const body = (cd.body || '').trim();
    if (!body || body === '[removed]' || body === '[deleted]') continue;
    if (cd.stickied) continue;
    if ((cd.author || '').toLowerCase() === 'automoderator') continue;
    if (body.length < 25) continue;
    comments.push({
      author: cd.author || '',
      score: cd.score || 0,
      body: body.replace(/\s+/g, ' ').slice(0, 400)
    });
  }
  comments.sort((a, b) => b.score - a.score);
  return comments.slice(0, limit || 5);
}

// ============================================================
// Summarization — Claude Haiku primary, Gemini fallback
// ============================================================
const PULSE_SYSTEM_PROMPT =
  'You are an audience-research analyst at Madison Gas and Electric (MGE), a Wisconsin utility. ' +
  'You read recent public Reddit commentary — both the original posts AND the top user replies — ' +
  'and produce a tight, neutral, editorial summary for the marketing and communications team. Hard rules: ' +
  '(1) Ground every claim in the actual posts and comments provided — do not invent quotes or stats. ' +
  '(2) Lean heavily on the COMMENTS (user replies) for sentiment and recurring concerns, not just the post titles. ' +
  '(3) Sentiment values: "positive" | "mixed" | "negative" | "low_signal" (use low_signal if fewer than 4 substantive posts). ' +
  '(4) Themes are 3-5 short noun-phrases describing what users are actually saying or asking about. ' +
  '(5) Summary is 2-3 sentences in plain professional tone — no marketing buzzwords, no hedging filler. ' +
  '(6) Notable quote: pick a real comment (preferred) or post snippet that captures a representative sentiment. ' +
  '(7) Output strict JSON only — no preamble, no markdown.';

function buildThemePrompt(theme, posts) {
  const top = posts.slice(0, 8);
  const blocks = top.map((p, i) => {
    const sourceLabel = p.source === 'reddit'
      ? 'r/' + p.subreddit + ', score ' + p.score + ', ' + p.comments + ' comments'
      : 'Bluesky, ' + p.score + ' likes, ' + p.comments + ' replies';
    let block = 'POST ' + (i + 1) + ' (' + sourceLabel + ')\n' +
                'Title: ' + (p.title || '').slice(0, 200);
    if (p.selftext && p.selftext.length > 30) {
      block += '\nBody: ' + p.selftext.replace(/\s+/g, ' ').slice(0, 300);
    }
    if (p.topComments && p.topComments.length > 0) {
      block += '\nTop user replies:';
      for (const c of p.topComments) {
        block += '\n  - (' + c.score + ' upvotes) "' + c.body + '"';
      }
    }
    return block;
  }).join('\n\n');

  return 'Theme: ' + theme.label + '\n\n' +
         'Recent posts and the actual user commentary on them (Reddit, last 30 days):\n\n' +
         blocks + '\n\n' +
         'Return strict JSON in exactly this shape — no other text:\n' +
         '{\n' +
         '  "sentiment": "positive" | "mixed" | "negative" | "low_signal",\n' +
         '  "summary": "2-3 sentence editorial summary grounded in what users are actually saying",\n' +
         '  "themes": ["short phrase", "short phrase", "short phrase"],\n' +
         '  "notable_quote": "one short representative quote — preferably from a user comment, not a post title"\n' +
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
      notable_quote: String(parsed.notable_quote || '').slice(0, 320)
    };
  } catch (e) {
    return { sentiment: 'mixed', summary: cleaned.slice(0, 400), themes: [], notable_quote: '' };
  }
}

async function callClaude(apiKey, userPrompt) {
  const resp = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
    body: JSON.stringify({
      model: HAIKU_MODEL,
      max_tokens: 700,
      system: PULSE_SYSTEM_PROMPT,
      messages: [{ role: 'user', content: userPrompt }]
    })
  });
  if (!resp.ok) {
    const errText = await resp.text();
    const err = new Error('Claude ' + resp.status + ': ' + errText.slice(0, 200));
    err.status = resp.status;
    throw err;
  }
  const payload = await resp.json();
  return (payload.content && payload.content[0] && payload.content[0].text) || '';
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
      generationConfig: { temperature: 0.4, maxOutputTokens: 800, responseMimeType: 'application/json' }
    })
  });
  if (!resp.ok) {
    const errText = await resp.text();
    const err = new Error('Gemini ' + resp.status + ': ' + errText.slice(0, 200));
    err.status = resp.status;
    throw err;
  }
  const payload = await resp.json();
  const parts = payload.candidates && payload.candidates[0] &&
                payload.candidates[0].content && payload.candidates[0].content.parts;
  return (parts && parts[0] && parts[0].text) || '';
}

async function withRetry(fn, label) {
  let lastErr;
  for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
    try { return await fn(); }
    catch (e) {
      lastErr = e;
      const transient = e.status === 429 || (e.status >= 500 && e.status < 600);
      if (!transient || attempt === MAX_RETRIES - 1) throw e;
      const wait = Math.pow(2, attempt) * 4000;
      console.warn(' [PULSE] ' + label + ' transient (' + e.status + '), backing off ' + wait + 'ms');
      await delay(wait);
    }
  }
  throw lastErr;
}

async function summarizeTheme(theme, posts) {
  const anthropicKey = process.env.ANTHROPIC_API_KEY;
  const geminiKey = process.env.GEMINI_API_KEY;
  if (!anthropicKey && !geminiKey) {
    return { sentiment: 'low_signal', summary: 'No LLM API key configured.', themes: [], notable_quote: '' };
  }
  if (posts.length === 0) {
    return { sentiment: 'low_signal', summary: 'No relevant posts found this week for this theme.', themes: [], notable_quote: '' };
  }
  const userPrompt = buildThemePrompt(theme, posts);

  // Prefer Claude (paid, reliable)
  if (anthropicKey) {
    try {
      const text = await withRetry(() => callClaude(anthropicKey, userPrompt), 'claude ' + theme.id);
      return parseModelOutput(text);
    } catch (e) {
      console.warn(' [PULSE] Claude permanently failed for ' + theme.id + ': ' + e.message);
      if (!geminiKey) return { sentiment: 'low_signal', summary: 'Claude call failed: ' + e.message.slice(0, 200), themes: [], notable_quote: '' };
    }
  }
  try {
    const text = await withRetry(() => callGemini(geminiKey, userPrompt), 'gemini ' + theme.id);
    return parseModelOutput(text);
  } catch (e) {
    return { sentiment: 'low_signal', summary: 'Summary generation failed: ' + e.message.slice(0, 200), themes: [], notable_quote: '' };
  }
}

// ============================================================
// Orchestrator
// ============================================================
async function generatePulse() {
  console.log(' [PULSE] Starting generation for week ' + getIsoWeekId(new Date()));
  const startedAt = Date.now();
  const themes = [];
  let lastClaudeCallAt = 0;

  for (const theme of PULSE_THEMES) {
    try {
      // 1. Reddit posts (subreddit-restricted, single combined OR search)
      const redditPosts = await fetchRedditPostsForTheme(theme);

      // 2. Top 8 by combined engagement
      const allPosts = redditPosts
        .sort((a, b) => (b.score + b.comments) - (a.score + a.comments));
      const top8 = allPosts.slice(0, 8);

      // 3. Fetch top comments for each
      for (const p of top8) {
        if (p.permalink) {
          p.topComments = await fetchTopCommentsForPost(p, 5);
          await delay(REDDIT_DELAY_MS);
        }
      }

      // 5. Rate-limit Claude calls
      const sinceLast = Date.now() - lastClaudeCallAt;
      if (sinceLast < CLAUDE_DELAY_MS) await delay(CLAUDE_DELAY_MS - sinceLast);
      lastClaudeCallAt = Date.now();

      // 6. Generate summary
      const ai = await summarizeTheme(theme, top8);

      // 7. Build sources list — preserve topComments so the UI can render user voice
      const sources = top8.map(p => ({
        source: p.source,
        title: p.title,
        subreddit: p.subreddit || '',
        author: p.author,
        score: p.score,
        comments: p.comments,
        url: p.url,
        createdAt: p.createdAt,
        topComments: (p.topComments || []).slice(0, 3) // surface 3 in UI to keep modal scannable
      }));

      themes.push({
        id: theme.id,
        label: theme.label,
        color: theme.color,
        postCount: allPosts.length,
        redditCount: redditPosts.length,
        blueskyCount: 0,
        commentsAnalyzed: top8.reduce((acc, p) => acc + ((p.topComments || []).length), 0),
        sentiment: ai.sentiment,
        summary: ai.summary,
        keyThemes: ai.themes,
        notableQuote: ai.notable_quote,
        sources: sources
      });
      console.log(' [PULSE]   ' + theme.label + ': ' + allPosts.length + ' posts, ' +
                  themes[themes.length - 1].commentsAnalyzed + ' comments, sentiment=' + ai.sentiment);
    } catch (e) {
      console.warn(' [PULSE] Theme ' + theme.id + ' failed:', e.message);
      themes.push({
        id: theme.id, label: theme.label, color: theme.color,
        postCount: 0, redditCount: 0, blueskyCount: 0, commentsAnalyzed: 0,
        sentiment: 'low_signal', summary: 'Generation failed: ' + e.message,
        keyThemes: [], notableQuote: '', sources: []
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
  if (!forceRefresh && cache && cache.weekId === currentWeek && !cache.error) return cache;
  if (_generationInFlight) return _generationInFlight;
  _generationInFlight = generatePulse()
    .catch(err => {
      console.warn(' [PULSE] Generation failed:', err.message);
      return cache || { weekId: currentWeek, generatedAt: null, themes: [], error: err.message };
    })
    .finally(() => { _generationInFlight = null; });
  return _generationInFlight;
}
function getCachedPulse() { return loadCache(); }
function maybeBackgroundGenerate() {
  const cache = loadCache();
  const currentWeek = getIsoWeekId(new Date());
  if (cache && cache.weekId === currentWeek && !cache.error) {
    console.log(' [PULSE] Cache fresh for week ' + currentWeek + ', no generation needed');
    return;
  }
  if (!process.env.ANTHROPIC_API_KEY && !process.env.GEMINI_API_KEY) {
    console.log(' [PULSE] Skipping background generation — no LLM key configured');
    return;
  }
  setTimeout(() => {
    console.log(' [PULSE] Triggering background generation');
    getPulseData().catch(err => console.warn(' [PULSE] Background generation failed:', err.message));
  }, 30000);
}
async function forceRegenerate() { return getPulseData(true); }

module.exports = {
  PULSE_THEMES, getPulseData, getCachedPulse, maybeBackgroundGenerate,
  forceRegenerate, getIsoWeekId
};
