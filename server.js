/**
 * MGE Social Media Command Center - Backend Server
 * Proxies API calls to Facebook, Instagram, LinkedIn, and YouTube
 */
const express = require('express');
const path = require('path');
const fs = require('fs');
const app = express();
app.use(express.json());

function loadConfig() {
  const fileConfig = (() => {
    try { return JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8')); }
    catch (e) { return null; }
  })();
  const config = {
    youtube: {
      enabled: process.env.YOUTUBE_API_KEY ? true : fileConfig?.youtube?.enabled || false,
      apiKey: process.env.YOUTUBE_API_KEY || fileConfig?.youtube?.apiKey || '',
      channelId: process.env.YOUTUBE_CHANNEL_ID || fileConfig?.youtube?.channelId || ''
    },
    facebook: {
      enabled: process.env.FACEBOOK_PAGE_TOKEN ? true : fileConfig?.facebook?.enabled || false,
      appId: process.env.FACEBOOK_APP_ID || fileConfig?.facebook?.appId || '',
      pageAccessToken: process.env.FACEBOOK_PAGE_TOKEN || fileConfig?.facebook?.pageAccessToken || '',
      pageId: process.env.FACEBOOK_PAGE_ID || fileConfig?.facebook?.pageId || ''
    },
    instagram: {
      enabled: process.env.INSTAGRAM_TOKEN ? true : fileConfig?.instagram?.enabled || false,
      accessToken: process.env.INSTAGRAM_TOKEN || fileConfig?.instagram?.accessToken || '',
      igUserId: process.env.INSTAGRAM_USER_ID || fileConfig?.instagram?.igUserId || ''
    },
    linkedin: {
      enabled: process.env.LINKEDIN_TOKEN ? true : fileConfig?.linkedin?.enabled || false,
      accessToken: process.env.LINKEDIN_TOKEN || fileConfig?.linkedin?.accessToken || '',
      organizationId: process.env.LINKEDIN_ORG_ID || fileConfig?.linkedin?.organizationId || ''
    },
    server: {
      port: parseInt(process.env.PORT || fileConfig?.server?.port || '3000', 10),
      refreshIntervalSeconds: fileConfig?.server?.refreshIntervalSeconds || 3600
    }
  };
  if (process.env.NODE_ENV !== 'production') {
    const usingEnv = !!(process.env.YOUTUBE_API_KEY || process.env.FACEBOOK_PAGE_TOKEN || process.env.INSTAGRAM_TOKEN || process.env.LINKEDIN_TOKEN);
    console.log(` Config source: ${usingEnv ? 'Environment variables' : 'config.json (or defaults)'}`);
  }
  return config;
}
let config = loadConfig();

app.use((req, res, next) => {
  if (req.path.startsWith('/api')) config = loadConfig();
  next();
});

const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD;
if (DASHBOARD_PASSWORD) {
  app.use((req, res, next) => {
    if (req.path === '/' || req.path === '/index.html' || req.path.endsWith('.css') || req.path.endsWith('.js') || req.path.endsWith('.html')) {
      const passwordParam = req.query.pw;
      const passwordCookie = req.cookies?.dashboardAuth;
      if (!passwordParam && !passwordCookie) {
        return res.send(`<!DOCTYPE html><html><head><title>MGE Social Command Center - Login</title><style>body{font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;background:#f5f5f5}.login-box{background:white;padding:40px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,0.1);width:100%;max-width:300px}h1{margin-top:0;color:#333;text-align:center}input{width:100%;padding:10px;margin:10px 0;border:1px solid #ddd;border-radius:4px;font-size:16px;box-sizing:border-box}button{width:100%;padding:10px;background:#007bff;color:white;border:none;border-radius:4px;cursor:pointer;font-size:16px}button:hover{background:#0056b3}</style></head><body><div class="login-box"><h1>MGE Social Command Center</h1><form method="GET" action="/"><input type="password" name="pw" placeholder="Enter dashboard password" required autofocus><button type="submit">Login</button></form></div></body></html>`);
      }
      if ((passwordParam || passwordCookie) && (passwordParam !== DASHBOARD_PASSWORD && passwordCookie !== DASHBOARD_PASSWORD)) {
        return res.status(403).send('Incorrect password');
      }
    }
    next();
  });
}

app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'MGE_Social_Command_Center.html')));

async function apiFetch(url, options = {}) {
  try {
    const resp = await fetch(url, options);
    if (!resp.ok) {
      const text = await resp.text();
      const isQuota = resp.status === 403 && text.toLowerCase().includes('quota');
      if (isQuota) console.warn(' [!] API quota exceeded for: ' + url.split('?')[0]);
      return { error: true, status: resp.status, message: text, quotaExceeded: isQuota };
    }
    return await resp.json();
  } catch (err) {
    return { error: true, message: err.message };
  }
}

app.get('/api/status', (req, res) => {
  res.json({
    youtube: !!(config.youtube?.enabled && config.youtube?.apiKey && config.youtube?.channelId),
    facebook: !!(config.facebook?.enabled && config.facebook?.pageAccessToken && config.facebook?.pageId),
    instagram: !!(config.instagram?.enabled && config.instagram?.accessToken && config.instagram?.igUserId),
    linkedin: !!(config.linkedin?.enabled && config.linkedin?.accessToken && config.linkedin?.organizationId),
    refreshInterval: config.server?.refreshIntervalSeconds || 3600
  });
});

function dateRangeParams(req) {
  const since = req.query.since;
  const until = req.query.until;
  let qs = '';
  if (since) qs += '&since=' + encodeURIComponent(since);
  if (until) qs += '&until=' + encodeURIComponent(until);
  return qs;
}

const YT_BASE = 'https://www.googleapis.com/youtube/v3';

app.get('/api/youtube/channel', async (req, res) => {
  const { apiKey, channelId } = config.youtube || {};
  if (!apiKey || !channelId) return res.json({ error: true, message: 'YouTube not configured' });
  const data = await apiFetch(`${YT_BASE}/channels?part=snippet,statistics,brandingSettings&id=${channelId}&key=${apiKey}`);
  res.json(data);
});

app.get('/api/youtube/videos', async (req, res) => {
  const { apiKey, channelId } = config.youtube || {};
  if (!apiKey || !channelId) return res.json({ error: true, message: 'YouTube not configured' });
  const channelData = await apiFetch(`${YT_BASE}/channels?part=contentDetails&id=${channelId}&key=${apiKey}`);
  if (channelData.error) return res.json(channelData);
  const uploadsId = channelData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  if (!uploadsId) return res.json({ error: true, message: 'No uploads playlist found' });
  const playlist = await apiFetch(`${YT_BASE}/playlistItems?part=snippet&playlistId=${uploadsId}&maxResults=50&key=${apiKey}`);
  if (playlist.error) return res.json(playlist);
  const videoIds = playlist.items?.map(i => i.snippet.resourceId.videoId).join(',');
  if (!videoIds) return res.json({ items: [] });
  const videos = await apiFetch(`${YT_BASE}/videos?part=snippet,statistics&id=${videoIds}&key=${apiKey}`);
  res.json(videos);
});

app.get('/api/youtube/comments/:videoId', async (req, res) => {
  const { apiKey } = config.youtube || {};
  if (!apiKey) return res.json({ error: true, message: 'YouTube not configured' });
  const data = await apiFetch(`${YT_BASE}/commentThreads?part=snippet&videoId=${req.params.videoId}&maxResults=50&order=time&key=${apiKey}`);
  res.json(data);
});

app.get('/api/youtube/all-comments', async (req, res) => {
  const { apiKey, channelId } = config.youtube || {};
  if (!apiKey || !channelId) return res.json({ error: true, message: 'YouTube not configured' });
  const channelData = await apiFetch(`${YT_BASE}/channels?part=contentDetails&id=${channelId}&key=${apiKey}`);
  if (channelData.error) return res.json(channelData);
  const uploadsId = channelData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  const playlist = await apiFetch(`${YT_BASE}/playlistItems?part=snippet&playlistId=${uploadsId}&maxResults=20&key=${apiKey}`);
  if (playlist.error) return res.json(playlist);
  const videoIds = playlist.items?.map(i => i.snippet.resourceId.videoId) || [];
  const commentPromises = videoIds.map(vid =>
    apiFetch(`${YT_BASE}/commentThreads?part=snippet&videoId=${vid}&maxResults=50&order=time&key=${apiKey}`)
  );
  const results = await Promise.all(commentPromises);
  const allComments = [];
  results.forEach((r, i) => {
    if (!r.error && r.items) {
      r.items.forEach(item => {
        const c = item.snippet.topLevelComment.snippet;
        allComments.push({
          platform: 'youtube',
          author: c.authorDisplayName,
          authorImage: c.authorProfileImageUrl,
          text: c.textDisplay,
          publishedAt: c.publishedAt,
          likeCount: c.likeCount,
          videoId: videoIds[i],
          videoTitle: playlist.items[i]?.snippet?.title || ''
        });
      });
    }
  });
  allComments.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  res.json({ comments: allComments });
});

const META_BASE = 'https://graph.facebook.com/v22.0';

app.get('/api/facebook/page', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });
  const data = await apiFetch(`${META_BASE}/${pageId}?fields=id,name,fan_count,followers_count,picture,about,engagement&access_token=${pageAccessToken}`);
  res.json(data);
});

app.get('/api/facebook/posts', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });
  let url = `${META_BASE}/${pageId}/posts?fields=id,message,created_time,full_picture,permalink_url,status_type,shares,reactions.summary(true),reactions.type(LIKE).limit(0).summary(total_count).as(reactions_like),reactions.type(LOVE).limit(0).summary(total_count).as(reactions_love),reactions.type(WOW).limit(0).summary(total_count).as(reactions_wow),reactions.type(HAHA).limit(0).summary(total_count).as(reactions_haha),reactions.type(SAD).limit(0).summary(total_count).as(reactions_sad),reactions.type(ANGRY).limit(0).summary(total_count).as(reactions_angry),reactions.type(CARE).limit(0).summary(total_count).as(reactions_care),comments.summary(true)&limit=50&access_token=${pageAccessToken}`;
  url += dateRangeParams(req);
  const data = await apiFetch(url);
  res.json(data);
});

app.get('/api/facebook/comments/:postId', async (req, res) => {
  const { pageAccessToken } = config.facebook || {};
  if (!pageAccessToken) return res.json({ error: true, message: 'Facebook not configured' });
  const data = await apiFetch(`${META_BASE}/${req.params.postId}/comments?fields=id,message,created_time,from,like_count,comment_count&limit=50&order=reverse_chronological&access_token=${pageAccessToken}`);
  res.json(data);
});

app.get('/api/facebook/all-comments', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });
  let url = `${META_BASE}/${pageId}/posts?fields=id,message,created_time,full_picture,comments{message,created_time,from,like_count}&limit=50&access_token=${pageAccessToken}`;
  url += dateRangeParams(req);
  const postsData = await apiFetch(url);
  if (postsData.error) return res.json(postsData);
  const allComments = [];
  (postsData.data || []).forEach(post => {
    const comments = post.comments?.data || [];
    comments.forEach(c => {
      allComments.push({
        platform: 'facebook',
        author: c.from?.name || 'Facebook User',
        text: c.message,
        publishedAt: c.created_time,
        likeCount: c.like_count || 0,
        postId: post.id,
        postMessage: post.message || '',
        postImage: post.full_picture || ''
      });
    });
  });
  allComments.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  res.json({ comments: allComments });
});

app.get('/api/facebook/insights', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });
  const metrics = 'page_impressions,page_engaged_users,page_post_engagements,page_fan_adds,page_views_total';
  const data = await apiFetch(`${META_BASE}/${pageId}/insights?metric=${metrics}&period=day&date_preset=last_30d&access_token=${pageAccessToken}`);
  res.json(data);
});

app.get('/api/instagram/profile', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });
  const data = await apiFetch(`${META_BASE}/${igUserId}?fields=id,name,username,profile_picture_url,followers_count,follows_count,media_count,biography&access_token=${accessToken}`);
  res.json(data);
});

app.get('/api/instagram/media', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });
  let url = `${META_BASE}/${igUserId}/media?fields=id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count&limit=50&access_token=${accessToken}`;
  if (req.query.since) { const sinceUnix = Math.floor(new Date(req.query.since).getTime() / 1000); url += '&since=' + sinceUnix; }
  if (req.query.until) { const untilUnix = Math.floor(new Date(req.query.until).getTime() / 1000); url += '&until=' + untilUnix; }
  const data = await apiFetch(url);
  res.json(data);
});

app.get('/api/instagram/comments/:mediaId', async (req, res) => {
  const { accessToken } = config.instagram || {};
  if (!accessToken) return res.json({ error: true, message: 'Instagram not configured' });
  const data = await apiFetch(`${META_BASE}/${req.params.mediaId}/comments?fields=id,text,timestamp,username,like_count&limit=50&access_token=${accessToken}`);
  res.json(data);
});

app.get('/api/instagram/all-comments', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });
  let url = `${META_BASE}/${igUserId}/media?fields=id,caption,media_url,thumbnail_url,timestamp,comments{text,timestamp,username,like_count}&limit=50&access_token=${accessToken}`;
  if (req.query.since) { const sinceUnix = Math.floor(new Date(req.query.since).getTime() / 1000); url += '&since=' + sinceUnix; }
  const mediaData = await apiFetch(url);
  if (mediaData.error) return res.json(mediaData);
  const allComments = [];
  (mediaData.data || []).forEach(post => {
    const comments = post.comments?.data || [];
    comments.forEach(c => {
      allComments.push({
        platform: 'instagram',
        author: c.username || 'Instagram User',
        text: c.text,
        publishedAt: c.timestamp,
        likeCount: c.like_count || 0,
        postId: post.id,
        postCaption: post.caption || '',
        postImage: post.media_url || post.thumbnail_url || ''
      });
    });
  });
  allComments.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  res.json({ comments: allComments });
});

app.get('/api/instagram/reach', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });
  let url = `${META_BASE}/${igUserId}/media?fields=id,timestamp,insights.metric(reach,impressions)&limit=50&access_token=${accessToken}`;
  if (req.query.since) { const sinceUnix = Math.floor(new Date(req.query.since).getTime() / 1000); url += '&since=' + sinceUnix; }
  if (req.query.until) { const untilUnix = Math.floor(new Date(req.query.until).getTime() / 1000); url += '&until=' + untilUnix; }
  const data = await apiFetch(url);
  res.json(data);
});

const LI_BASE = 'https://api.linkedin.com/v2';
const LI_REST = 'https://api.linkedin.com/rest';

app.get('/api/linkedin/organization', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });
  const data = await apiFetch(
    `${LI_BASE}/organizations/${organizationId}?projection=(id,localizedName,vanityName,logoV2,followersCount)`,
    { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503', 'X-Restli-Protocol-Version': '2.0.0' } }
  );
  res.json(data);
});

app.get('/api/linkedin/follower-count', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });
  const data = await apiFetch(
    `${LI_BASE}/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:${organizationId}`,
    { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503', 'X-Restli-Protocol-Version': '2.0.0' } }
  );
  res.json(data);
});

app.get('/api/linkedin/posts', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });
  const data = await apiFetch(
    `${LI_REST}/posts?author=urn:li:organization:${organizationId}&q=author&count=50&sortBy=LAST_MODIFIED`,
    { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' } }
  );
  res.json(data);
});

app.get('/api/linkedin/social-actions/:postUrn', async (req, res) => {
  const { accessToken } = config.linkedin || {};
  if (!accessToken) return res.json({ error: true, message: 'LinkedIn not configured' });
  const urn = decodeURIComponent(req.params.postUrn);
  const [likes, comments] = await Promise.all([
    apiFetch(`${LI_REST}/socialActions/${encodeURIComponent(urn)}/likes?count=50`, { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' } }),
    apiFetch(`${LI_REST}/socialActions/${encodeURIComponent(urn)}/comments?count=50`, { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' } })
  ]);
  res.json({ likes, comments });
});

app.get('/api/linkedin/page-stats', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });
  const data = await apiFetch(
    `${LI_REST}/organizationPageStatistics?q=organization&organization=urn:li:organization:${organizationId}`,
    { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' } }
  );
  res.json(data);
});

app.get('/api/linkedin/all-comments', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });
  const postsData = await apiFetch(
    `${LI_REST}/posts?author=urn:li:organization:${organizationId}&q=author&count=50&sortBy=LAST_MODIFIED`,
    { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' } }
  );
  if (postsData.error) return res.json(postsData);
  const posts = postsData.elements || [];
  const commentPromises = posts.map(p =>
    apiFetch(`${LI_REST}/socialActions/${encodeURIComponent(p.id)}/comments?count=50`, { headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' } })
  );
  const results = await Promise.all(commentPromises);
  const allComments = [];
  results.forEach((r, i) => {
    if (!r.error && r.elements) {
      r.elements.forEach(c => {
        allComments.push({
          platform: 'linkedin',
          author: c.actor?.name || 'LinkedIn User',
          text: c.message?.text || c.comment || '',
          publishedAt: c.created?.time ? new Date(c.created.time).toISOString() : new Date().toISOString(),
          likeCount: c.likeCount || 0,
          postId: posts[i].id,
          postText: posts[i].commentary || posts[i].specificContent?.['com.linkedin.ugc.ShareContent']?.shareCommentary?.text || ''
        });
      });
    }
  });
  allComments.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  res.json({ comments: allComments });
});

app.get('/api/all-data', async (req, res) => {
  const status = {
    youtube: !!(config.youtube?.enabled && config.youtube?.apiKey),
    facebook: !!(config.facebook?.enabled && config.facebook?.pageAccessToken),
    instagram: !!(config.instagram?.enabled && config.instagram?.accessToken),
    linkedin: !!(config.linkedin?.enabled && config.linkedin?.accessToken)
  };
  const port = config.server?.port || 3000;
  const fetches = {};
  if (status.youtube) {
    fetches.youtubeChannel = apiFetch(`http://localhost:${port}/api/youtube/channel`);
    fetches.youtubeVideos = apiFetch(`http://localhost:${port}/api/youtube/videos`);
    fetches.youtubeComments = apiFetch(`http://localhost:${port}/api/youtube/all-comments`);
  }
  if (status.facebook) {
    fetches.facebookPage = apiFetch(`http://localhost:${port}/api/facebook/page`);
    fetches.facebookPosts = apiFetch(`http://localhost:${port}/api/facebook/posts`);
    fetches.facebookComments = apiFetch(`http://localhost:${port}/api/facebook/all-comments`);
    fetches.facebookInsights = apiFetch(`http://localhost:${port}/api/facebook/insights`);
  }
  if (status.instagram) {
    fetches.instagramProfile = apiFetch(`http://localhost:${port}/api/instagram/profile`);
    fetches.instagramMedia = apiFetch(`http://localhost:${port}/api/instagram/media`);
    fetches.instagramComments = apiFetch(`http://localhost:${port}/api/instagram/all-comments`);
    fetches.instagramReach = apiFetch(`http://localhost:${port}/api/instagram/reach`);
  }
  if (status.linkedin) {
    fetches.linkedinOrg = apiFetch(`http://localhost:${port}/api/linkedin/organization`);
    fetches.linkedinPosts = apiFetch(`http://localhost:${port}/api/linkedin/posts`);
    fetches.linkedinComments = apiFetch(`http://localhost:${port}/api/linkedin/all-comments`);
    fetches.linkedinPageStats = apiFetch(`http://localhost:${port}/api/linkedin/page-stats`);
  }
  const keys = Object.keys(fetches);
  const values = await Promise.all(Object.values(fetches));
  const result = { status };
  keys.forEach((k, i) => result[k] = values[i]);
  res.json(result);
});


// ============================================================
// MEDIA MONITORING & MENTIONS
// Adds /api/mentions + 5 source pollers (Reddit, Google Alerts,
// GDELT, YouTube keyword search, local news RSS). All polling
// runs in-process on staggered intervals; results cached in memory.
// ============================================================

const Parser = require('rss-parser');
const rssParser = new Parser({
  timeout: 15000,
  headers: { 'User-Agent': 'MGE-Social-Command-Center/1.0 (Madison Gas and Electric brand monitoring)' },
  customFields: {
    item: [
      ['media:thumbnail', 'mediaThumbnail'],
      ['media:content', 'mediaContent'],
      ['content:encoded', 'contentEncoded'],
      ['og:image', 'ogImage']
    ]
  }
});

// --- Image/thumbnail extraction helpers ---
function firstImgFromHtml(html) {
  if (!html) return null;
  const m = String(html).match(/<img[^>]+src=["']([^"']+)["']/i);
  return m ? m[1] : null;
}

function imageFromRssItem(item) {
  // enclosure (standard)
  if (item.enclosure && item.enclosure.url && (!item.enclosure.type || /^image\//i.test(item.enclosure.type))) {
    return item.enclosure.url;
  }
  // media:thumbnail and media:content (rss-parser exposes attributes as $ object)
  const mt = item.mediaThumbnail;
  if (mt) {
    if (typeof mt === 'string') return mt;
    if (mt.$ && mt.$.url) return mt.$.url;
    if (Array.isArray(mt) && mt[0] && mt[0].$) return mt[0].$.url;
  }
  const mc = item.mediaContent;
  if (mc) {
    if (typeof mc === 'string') return mc;
    if (mc.$ && mc.$.url && (!mc.$.medium || mc.$.medium === 'image')) return mc.$.url;
    if (Array.isArray(mc)) {
      for (const c of mc) {
        if (c && c.$ && c.$.url && (!c.$.medium || c.$.medium === 'image')) return c.$.url;
      }
    }
  }
  // Inline <img> in content:encoded or content
  const img = firstImgFromHtml(item.contentEncoded) || firstImgFromHtml(item.content);
  if (img) return img;
  return null;
}

// --- Job listing filters (exclude MGE job postings/career listings) ---
const JOB_URL_PATTERNS = /(jobs?\.mge|careers?\.mge|\/careers?\/|\/jobs?\/|\/job-openings|indeed\.com|glassdoor\.com\/job|ziprecruiter\.com|monster\.com|simplyhired\.com|linkedin\.com\/jobs|jobvite\.com|myworkdayjobs\.com|greenhouse\.io|lever\.co|icims\.com|taleo\.net|ultipro\.com|paylocity\.com|adp\.com\/careers|smartrecruiters\.com|bamboohr\.com\/jobs|recruiting\.\w+\.(com|org))/i;
const JOB_TEXT_PATTERNS = /\b(apply (now|today|online|here)|job (opening|openings|posting|postings|description|id)|now hiring|we'?re hiring|hiring for|currently hiring|open position|open positions|current openings|join (our|the) team|career opportunit(y|ies)|employment opportunit(y|ies)|full[- ]?time position|part[- ]?time position|equal opportunity employer|competitive (salary|benefits|pay)|salary range|compensation range|pay range: \$|\$\d+[\s\S]{0,20}per (hour|year))\b/i;

function isJobListing(m) {
  try {
    if (m.url && JOB_URL_PATTERNS.test(m.url)) return true;
  } catch {}
  const text = (m.title || '') + ' ' + (m.snippet || '');
  if (JOB_TEXT_PATTERNS.test(text)) return true;
  return false;
}

// --- Brand keyword matching with Wisconsin geo-disambiguation ---
const UTILITY_CONTEXT = /\b(outage|power|electric|gas|utility|bill|rate|customer|energy|meter|grid|blackout|restoration|substation|transformer|kilowatt|kwh|solar|renewable)\b/i;
const WISCONSIN_GEO = /\b(Wisconsin|Madison|Dane County|Sun Prairie|Middleton|Fitchburg|Monona|Verona|Waunakee|Stoughton|Cottage Grove|McFarland|Milwaukee|Green Bay|Appleton|Eau Claire|Kenosha|Racine|Oshkosh|Janesville|La Crosse)\b/i;

// Brand configurations: each utility, with strict patterns and optional ambiguous+context disambiguation
// "requiresContext" means the text must ALSO mention utility terms or the utility's service-area geo
// (used to filter out false positives like sports arenas, stock tickers, unrelated orgs)
const BRANDS = {
  mge: {
    exact: [/\bMadison Gas and Electric\b/i, /\bMadison Gas & Electric\b/i, /\bMG&E\b/i, /\bMGE Energy\b/i],
    ambiguous: /\bMGE\b/,
    negative: /\bMGE Wealth|MGE Group|Mitsubishi|MGE Capital\b/i,
    requiresContext: true,
    contextRegex: null // uses default UTILITY_CONTEXT + WISCONSIN_GEO
  },
  alliant: {
    exact: [/\bAlliant Energy\b/i, /\bAlliant Energy Corporation\b/i, /\bInterstate Power (and|&) Light\b/i, /\bWisconsin Power (and|&) Light\b/i],
    ambiguous: null,
    negative: /\bAlliant International|Alliant University|Alliant Insurance|Alliant Credit Union|Alliant Techsystems|Alliant Capital\b/i,
    requiresContext: false,
    contextRegex: null
  },
  we_energies: {
    exact: [/\bWe Energies\b/i, /\bWisconsin Electric Power\b/i, /\bWEC Energy Group\b/i, /\bWisconsin Gas LLC\b/i],
    ambiguous: null,
    negative: null,
    requiresContext: false,
    contextRegex: null
  }
  // Xcel Energy intentionally removed — was surfacing inappropriate / off-topic content
  // (stock-bot signals, sports-arena references). Can be re-added later with stricter rules.
};

// Domains to exclude from all brand matching (stock signal bots, content farms, etc.)
const EXCLUDED_DOMAINS = /(getagraph\.com|tickercrunch|marketbeat\.com\/stock-ideas|zacks\.com\/stock\/news|simplywall\.st|insidermonkey\.com|stockstotrade|stocknews\.com|stockinvest|nasdaq\.com\/articles\/.*stock|benzinga\.com\/quote|fool\.com\/quote)/i;

// Strict energy/utility signal — for topical items to be accepted, their text must hit one of these.
// Broad enough to catch industry trends (renewables, solar, grid mod, EVs, decarbonization)
// but specific enough to filter out political/general news.
const TOPICAL_STRICT_SIGNALS = /\b(power outage|power outages|outage|outages|no power|blackout|blackouts|kilowatt|kwh|megawatt|mwh|solar panels?|solar install\w*|solar array|rooftop solar|solar company|solar companies|solar energy|solar power|solar farm|solar farms|utility[- ]scale solar|community solar|solar rebate|solar incentive|solar tariff|install solar|going solar|heat pump|heat pumps|ev charger|ev chargers|ev charging|electric vehicle|electric vehicles|electric vehicle charg\w+|charging station\w*|utility bill|utility bills|electric bill|electric bills|gas bill|gas bills|rate increase|rate hike|rate case|rate filing|net metering|time[- ]of[- ]use|tou rate|focus on energy|smart meter|smart meters|smart grid|grid modern\w+|grid reliability|electric grid|substation|transformer|public service commission|psc wisconsin|natural gas|nuclear plant|point beach|kewaunee|small modular reactor|\bsmr\b|wind farm|wind farms|wind turbine|wind turbines|wind energy|wind power|offshore wind|renewable energy|renewable power|renewables|clean energy|clean power|green energy|energy efficiency|weatherization|energy assistance|liheap|shut off|shutoff|power restored|power restoration|battery storage|energy storage|grid[- ]scale battery|lithium battery|demand response|virtual power plant|distributed energy|distributed generation|microgrid|microgrids|electrification|beneficial electrification|net zero|net[- ]zero|decarbonization|decarboniz\w+|carbon capture|carbon neutral|clean hydrogen|hydrogen fuel|hydrogen power|power purchase agreement|\bppa\b|energy transition|green hydrogen|grid stability|peak demand|load shed\w+|inflation reduction act|ira tax credit|investment tax credit|production tax credit)\b/i;

// Political/general-news noise — if topical text matches this AND doesn't have strong utility signal,
// reject it (catches Tammy Baldwin, election coverage, general Madison news).
const POLITICAL_NOISE = /\b(tammy baldwin|ron johnson|tony evers|derrick van orden|gwen moore|mark pocan|kamala harris|donald trump|joe biden|senator|congressm\w+|congresswom\w+|state assembly|state senator|state representative|election\w*|campaign\w*|primary|midterm|partisan|ballot|voter|voting|poll\w*|house of representatives|gubernatorial|state capitol|governor's office|state of the union|impeach\w+|indict\w+|supreme court|scotus)\b/i;

function verifyTopicalContent(text) {
  if (!text) return false;
  const hasStrictSignal = TOPICAL_STRICT_SIGNALS.test(text);
  if (!hasStrictSignal) return false;
  // Even with strict signal, if political noise dominates, reject
  const signalMatches = (text.match(new RegExp(TOPICAL_STRICT_SIGNALS.source, 'gi')) || []).length;
  const politicalMatches = (text.match(new RegExp(POLITICAL_NOISE.source, 'gi')) || []).length;
  // Require at least as many strict signals as political references
  if (politicalMatches > signalMatches) return false;
  return true;
}

// Identify which brand a piece of text matches (if any)
// Returns { brand, keyword, confidence } or null
//
// Signature: matchesAnyBrand(text, url, [title])
//   - text: full text (usually title + body combined)
//   - url: source URL for domain blacklist check
//   - title: (optional) when provided, enables title-aware mode for news feeds.
//            Title match = high confidence, single pass. Body-only match requires
//            2+ mentions to filter out "related articles" footer-link noise.
//            When omitted (Reddit, YouTube), old single-pass behavior applies.
function matchesAnyBrand(text, url, title) {
  if (!text) return null;
  // Domain exclusion first — skip stock-signal and low-quality sources entirely
  if (url && EXCLUDED_DOMAINS.test(url)) return null;

  const titleAware = (typeof title === 'string' && title.length > 0);
  const titleText = titleAware ? title : '';

  for (const [tag, cfg] of Object.entries(BRANDS)) {
    if (cfg.negative && cfg.negative.test(text)) continue;

    // Exact match (strong brand signal)
    for (const p of cfg.exact) {
      let matchedKeyword = null;
      let matchConfidence = 'high';
      let matchedInTitle = false;

      if (titleAware) {
        // Title-aware mode: title match is authoritative.
        const tm = titleText.match(p);
        if (tm) {
          matchedKeyword = tm[0];
          matchedInTitle = true;
        } else {
          // Not in title — require 2+ occurrences in full text to accept
          // (single body match is usually a footer/related-articles link)
          const globalP = new RegExp(p.source, 'gi');
          const allMatches = text.match(globalP) || [];
          if (allMatches.length >= 2) {
            matchedKeyword = allMatches[0];
            matchConfidence = 'medium';
          }
        }
      } else {
        // Legacy single-match mode (Reddit self-posts, YouTube descriptions)
        const m = text.match(p);
        if (m) matchedKeyword = m[0];
      }

      if (matchedKeyword) {
        // Some brands (Xcel) require context — still enforce
        if (cfg.requiresContext && cfg.contextRegex) {
          const u = UTILITY_CONTEXT.test(text);
          const g = cfg.contextRegex.test(text);
          if (!u && !g) continue;
          return { brand: tag, keyword: matchedKeyword, confidence: (u && g) ? 'high' : matchConfidence };
        }
        return { brand: tag, keyword: matchedKeyword, confidence: matchConfidence };
      }
    }

    // Ambiguous match (needs context disambiguation)
    if (cfg.ambiguous && cfg.ambiguous.test(text)) {
      const u = UTILITY_CONTEXT.test(text);
      const ctxRegex = cfg.contextRegex || WISCONSIN_GEO;
      const g = ctxRegex.test(text);
      if (u || g) {
        // In title-aware mode, also require that the ambiguous term appears in title
        // OR multiple times in body (single body mention is almost always a footer link)
        if (titleAware) {
          const inTitle = cfg.ambiguous.test(titleText);
          if (!inTitle) {
            const globalA = new RegExp(cfg.ambiguous.source, 'gi');
            const allMatches = text.match(globalA) || [];
            if (allMatches.length < 2) continue;
          }
        }
        const keyword = (text.match(cfg.ambiguous) || [tag])[0];
        return { brand: tag, keyword: keyword, confidence: (u && g) ? 'high' : 'medium' };
      }
    }
  }
  return null;
}

// Backwards-compatible alias (MGE-only match) used by existing pollers
function matchesBrand(text) {
  const m = matchesAnyBrand(text);
  if (!m || m.brand !== 'mge') return null;
  return { keyword: m.keyword, confidence: m.confidence };
}

// --- In-memory cache (no DB, intentional for free-tier footprint) ---
const MENTIONS = {
  items: [],
  lastPoll: {},
  stats: { reddit: 0, google_alerts: 0, gdelt: 0, youtube_search: 0, local_news: 0, google_news: 0, industry_news: 0, sec_filings: 0, podcasts: 0 },
  brandStats: { mge: 0, alliant: 0, we_energies: 0, topical: 0 },
  maxSize: 800
};

function addMentions(source, incoming) {
  MENTIONS.lastPoll[source] = new Date().toISOString();
  if (!incoming || incoming.length === 0) return 0;
  // Filter out job postings/careers listings
  const beforeJob = incoming.length;
  incoming = incoming.filter(m => !isJobListing(m));
  const jobSkipped = beforeJob - incoming.length;
  if (jobSkipped > 0) console.log(' [MENTIONS] ' + source + ': filtered ' + jobSkipped + ' job listing(s)');
  // Dedupe within incoming by id (the same item can match multiple poll queries)
  const seen = new Set();
  incoming = incoming.filter(i => {
    if (!i || !i.id) return false;
    if (seen.has(i.id)) return false;
    seen.add(i.id);
    return true;
  });
  const existing = new Set(MENTIONS.items.map(m => m.id));
  const additions = incoming.filter(i => !existing.has(i.id));
  if (additions.length === 0) return 0;
  MENTIONS.items = [...additions, ...MENTIONS.items]
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt))
    .slice(0, MENTIONS.maxSize);
  MENTIONS.stats[source] = (MENTIONS.stats[source] || 0) + additions.length;
  for (const a of additions) {
    const bt = a.brandTag || 'mge';
    MENTIONS.brandStats[bt] = (MENTIONS.brandStats[bt] || 0) + 1;
  }
  console.log(' [MENTIONS] ' + source + ': +' + additions.length + ' (cache total: ' + MENTIONS.items.length + ')');
  return additions.length;
}

function cleanHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ').replace(/&amp;/g, '&').replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>').replace(/&quot;/g, '"').replace(/&#39;/g, "'")
    .replace(/\s+/g, ' ').trim();
}

// --- 1. Reddit (no-auth public JSON endpoints) ---
// Tagged queries: each entry carries a default brandTag for all matches.
// Brand queries still run through matchesAnyBrand() for verification; topical
// queries are scoped to Wisconsin subs and accepted without strict brand match.
const REDDIT_QUERIES = [
  // --- MGE brand (high priority, full year) ---
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+greenbay+milwaukee+Appleton+EauClaire+LaCrosse/search.json?q=%22Madison+Gas+and+Electric%22+OR+%22MG%26E%22+OR+MGE&restrict_sr=1&sort=new&limit=25&t=year', tag: 'mge', requireBrand: true },
  { url: 'https://www.reddit.com/search.json?q=%22Madison+Gas+and+Electric%22+OR+%22MG%26E%22&sort=new&limit=25&t=year', tag: 'mge', requireBrand: true },
  // --- Competitors ---
  { url: 'https://www.reddit.com/r/wisconsin+milwaukee+madisonwi+greenbay+Appleton/search.json?q=%22Alliant+Energy%22&restrict_sr=1&sort=new&limit=20&t=year', tag: 'alliant', requireBrand: true },
  { url: 'https://www.reddit.com/search.json?q=%22Alliant+Energy%22+Wisconsin&sort=new&limit=20&t=year', tag: 'alliant', requireBrand: true },
  { url: 'https://www.reddit.com/r/wisconsin+milwaukee+madisonwi+greenbay+Appleton/search.json?q=%22We+Energies%22&restrict_sr=1&sort=new&limit=20&t=year', tag: 'we_energies', requireBrand: true },
  { url: 'https://www.reddit.com/search.json?q=%22We+Energies%22&sort=new&limit=20&t=year', tag: 'we_energies', requireBrand: true },
  // --- Topical (scoped to WI subs; surface for context even without brand name) ---
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee+greenbay/search.json?q=%22power+outage%22+OR+%22power+out%22+OR+blackout+OR+%22no+power%22&restrict_sr=1&sort=new&limit=20&t=month', tag: 'topical', requireBrand: false },
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee/search.json?q=%22solar+energy%22+OR+%22solar+panels%22+OR+%22solar+company%22+OR+%22solar+install%22+OR+%22go+solar%22+OR+%22rooftop+solar%22+OR+%22solar+farm%22+OR+%22community+solar%22&restrict_sr=1&sort=new&limit=20&t=year', tag: 'topical', requireBrand: false },
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee/search.json?q=%22renewable+energy%22+OR+%22clean+energy%22+OR+%22green+energy%22+OR+%22energy+efficiency%22+OR+%22wind+energy%22+OR+%22wind+power%22+OR+%22wind+farm%22&restrict_sr=1&sort=new&limit=15&t=year', tag: 'topical', requireBrand: false },
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee/search.json?q=%22electric+bill%22+OR+%22gas+bill%22+OR+%22utility+bill%22+OR+%22rate+increase%22+OR+%22rate+hike%22+OR+%22rate+case%22&restrict_sr=1&sort=new&limit=15&t=year', tag: 'topical', requireBrand: false },
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee/search.json?q=%22heat+pump%22+OR+%22EV+charger%22+OR+%22electric+vehicle%22+OR+%22charging+station%22+OR+%22focus+on+energy%22&restrict_sr=1&sort=new&limit=15&t=year', tag: 'topical', requireBrand: false },
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee/search.json?q=%22battery+storage%22+OR+%22energy+storage%22+OR+%22grid+modernization%22+OR+%22smart+meter%22+OR+%22net+metering%22+OR+%22decarbonization%22+OR+%22electrification%22&restrict_sr=1&sort=new&limit=10&t=year', tag: 'topical', requireBrand: false },
  { url: 'https://www.reddit.com/r/madisonwi+wisconsin+madison+milwaukee/search.json?q=%22solar+installer%22+OR+%22solar+tax+credit%22+OR+%22solar+rebate%22+OR+%22Inflation+Reduction+Act%22+OR+%22clean+energy+jobs%22&restrict_sr=1&sort=new&limit=10&t=year', tag: 'topical', requireBrand: false }
];

async function pollReddit() {
  let totalFetched = 0;
  let totalErrors = 0;
  const found = [];
  for (const q of REDDIT_QUERIES) {
    try {
      const resp = await fetch(q.url, {
        headers: {
          'User-Agent': 'MGE-Social-Command-Center/1.0 (by u/taylormcgraham; Madison Gas and Electric brand monitoring)',
          'Accept': 'application/json'
        }
      });
      if (!resp.ok) {
        console.warn(' [MENTIONS] Reddit query ' + resp.status + ': ' + q.url.substring(0, 80));
        totalErrors++;
        continue;
      }
      const data = await resp.json();
      const children = (data && data.data && data.data.children) || [];
      totalFetched += children.length;
      for (const c of children) {
        const d = c.data;
        if (!d || !d.id) continue;
        const text = (d.title || '') + ' ' + (d.selftext || '');
        const permalink = 'https://www.reddit.com' + (d.permalink || '');
        const externalUrl = d.url_overridden_by_dest || '';
        let brandTag = q.tag;
        let matchedKeyword = q.tag;
        let confidence = 'medium';

        // Check external link for excluded domains too (catches stock bots posting to Reddit)
        if (externalUrl && EXCLUDED_DOMAINS.test(externalUrl)) continue;

        if (q.requireBrand) {
          const m = matchesAnyBrand(text, externalUrl || permalink);
          if (!m || m.brand !== q.tag) continue;
          matchedKeyword = m.keyword;
          confidence = m.confidence;
        } else {
          // Topical query — require strict energy signal (not just political/general mentions)
          const m = matchesAnyBrand(text, externalUrl || permalink);
          if (m) {
            // Still accept if it matches a known brand — that's always valid
            brandTag = m.brand;
            matchedKeyword = m.keyword;
            confidence = m.confidence;
          } else {
            // For pure topical, require the text to hit the strict signal list
            // AND have more utility signals than political noise
            if (!verifyTopicalContent(text)) continue;
            // Use the first strict-signal match as the displayed keyword
            const sig = text.match(TOPICAL_STRICT_SIGNALS);
            matchedKeyword = sig ? sig[0] : 'energy topic';
            confidence = 'topical';
          }
        }

        // Thumbnail extraction
        let thumb = null;
        if (d.thumbnail && !/^(self|default|nsfw|spoiler|image|)$/.test(d.thumbnail) && /^https?:/.test(d.thumbnail)) {
          thumb = d.thumbnail;
        }
        if (!thumb && d.preview && d.preview.images && d.preview.images[0]) {
          const src = d.preview.images[0].source && d.preview.images[0].source.url;
          if (src) thumb = String(src).replace(/&amp;/g, '&');
        }
        if (!thumb && d.url_overridden_by_dest && /\.(jpe?g|png|gif|webp)(\?|$)/i.test(d.url_overridden_by_dest)) {
          thumb = d.url_overridden_by_dest;
        }

        // External URL: if this is a link post (not self-post), store where it points
        let extUrl = null;
        if (d.url_overridden_by_dest &&
            !d.url_overridden_by_dest.startsWith('https://www.reddit.com') &&
            !d.url_overridden_by_dest.startsWith('https://reddit.com') &&
            !d.url_overridden_by_dest.startsWith('/r/')) {
          extUrl = d.url_overridden_by_dest;
        }
        found.push({
          id: 'reddit:' + d.id,
          source: 'reddit',
          sourceDisplay: 'Reddit \u00b7 r/' + d.subreddit,
          sourceName: 'r/' + d.subreddit,
          title: d.title || '',
          snippet: (d.selftext || '').substring(0, 300),
          url: 'https://www.reddit.com' + d.permalink,
          externalUrl: extUrl,
          author: 'u/' + (d.author || 'unknown'),
          publishedAt: new Date((d.created_utc || Date.now() / 1000) * 1000).toISOString(),
          thumbnail: thumb,
          brandTag: brandTag,
          matchedKeyword: matchedKeyword,
          confidence: confidence,
          engagement: { score: d.score || 0, comments: d.num_comments || 0 }
        });
      }
      // Small delay between Reddit queries so we don't trip rate limits
      await new Promise(r => setTimeout(r, 800));
    } catch (err) {
      console.warn(' [MENTIONS] Reddit query error:', err.message);
      totalErrors++;
    }
  }
  console.log(' [MENTIONS] Reddit: ' + REDDIT_QUERIES.length + ' queries, ' + totalFetched + ' posts fetched, ' + found.length + ' matched, ' + totalErrors + ' errors');
  addMentions('reddit', found);
}

// --- 2. Google Alerts RSS (Taylor creates the alerts, we poll URLs from env) ---
function extractGalertUrl(wrapped) {
  if (!wrapped) return null;
  try { return new URL(wrapped).searchParams.get('url') || null; } catch { return null; }
}

async function pollGoogleAlerts() {
  const urls = (process.env.GOOGLE_ALERTS_RSS_URLS || '').split(',').map(s => s.trim()).filter(Boolean);
  if (urls.length === 0) { MENTIONS.lastPoll.google_alerts = new Date().toISOString(); return; }
  const found = [];
  for (const url of urls) {
    try {
      const feed = await rssParser.parseURL(url);
      for (const item of (feed.items || [])) {
        const actualUrl = extractGalertUrl(item.link) || item.link;
        const titleText = cleanHtml(item.title || '');
        const bodyText = cleanHtml(item.contentSnippet || item.content || '');
        const text = titleText + ' ' + bodyText;
        const m = matchesAnyBrand(text, actualUrl, titleText);
        if (!m) continue;
        let domain = 'Google Alerts';
        try { domain = new URL(actualUrl).hostname.replace(/^www\./, ''); } catch {}
        found.push({
          id: 'galerts:' + (item.guid || actualUrl),
          source: 'google_alerts',
          sourceDisplay: 'Google Alerts \u00b7 ' + domain,
          sourceName: domain,
          title: cleanHtml(item.title || ''),
          snippet: cleanHtml(item.contentSnippet || item.content || '').substring(0, 300),
          url: actualUrl,
          author: domain,
          publishedAt: item.isoDate || item.pubDate || new Date().toISOString(),
          thumbnail: imageFromRssItem(item),
          brandTag: m.brand,
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    } catch (err) {
      console.warn(' [MENTIONS] Google Alert feed failed:', err.message);
    }
  }
  addMentions('google_alerts', found);
}

// --- 3. GDELT Doc 2.0 (free news API) ---
function parseGdeltDate(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/);
  return m ? `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6]}Z` : null;
}

async function pollGDELT() {
  try {
    const queries = [
      { q: '"Madison Gas and Electric"', tag: 'mge' },
      { q: '"Madison Gas & Electric"', tag: 'mge' },
      { q: '"MG&E" Wisconsin', tag: 'mge' },
      { q: '"Alliant Energy" Wisconsin', tag: 'alliant' },
      { q: '"We Energies"', tag: 'we_energies' }
    ];
    const found = [];
    for (const { q, tag } of queries) {
      const url = `https://api.gdeltproject.org/api/v2/doc/doc?query=${encodeURIComponent(q)}&mode=ArtList&format=json&maxrecords=75&sort=DateDesc&timespan=3months`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const body = await resp.text();
      let data; try { data = JSON.parse(body); } catch { continue; }
      for (const a of (data.articles || [])) {
        const m = matchesAnyBrand(a.title || '', a.url);
        if (!m || m.brand !== tag) continue;
        found.push({
          id: 'gdelt:' + (a.url || a.documentidentifier || a.title),
          source: 'gdelt',
          sourceDisplay: 'GDELT \u00b7 ' + (a.domain || 'news'),
          sourceName: a.domain || 'News',
          title: a.title || '',
          snippet: '',
          url: a.url,
          author: a.domain || 'Unknown',
          publishedAt: parseGdeltDate(a.seendate) || new Date().toISOString(),
          thumbnail: a.socialimage || null,
          brandTag: m.brand,
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    }
    console.log(' [MENTIONS] GDELT: ' + found.length + ' matched across ' + queries.length + ' queries (3-month window)');
    addMentions('gdelt', found);
  } catch (err) {
    console.warn(' [MENTIONS] GDELT failed:', err.message);
  }
}

// --- 4. YouTube keyword search (reuses existing YOUTUBE_API_KEY) ---
async function pollYouTubeMentions() {
  const apiKey = (typeof config !== 'undefined' && config.youtube && config.youtube.apiKey) || process.env.YOUTUBE_API_KEY;
  if (!apiKey) { MENTIONS.lastPoll.youtube_search = new Date().toISOString(); return; }
  try {
    // Limit to last 90 days for YouTube search so we get the 3-month window
    const publishedAfter = new Date(Date.now() - 90 * 86400000).toISOString();
    const queries = [
      { q: '"Madison Gas and Electric"', tag: 'mge' },
      { q: '"MG&E" Wisconsin', tag: 'mge' },
      { q: '"Alliant Energy" Wisconsin', tag: 'alliant' },
      { q: '"We Energies"', tag: 'we_energies' }
    ];
    const found = [];
    for (const { q, tag } of queries) {
      const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(q)}&type=video&order=date&maxResults=20&publishedAfter=${encodeURIComponent(publishedAfter)}&key=${apiKey}`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const data = await resp.json();
      for (const v of (data.items || [])) {
        const s = v.snippet || {};
        const vurl = 'https://www.youtube.com/watch?v=' + (v.id && v.id.videoId);
        const m = matchesAnyBrand((s.title || '') + ' ' + (s.description || ''), vurl);
        if (!m || m.brand !== tag) continue;
        const thumbs = s.thumbnails || {};
        const thumb = (thumbs.high && thumbs.high.url) || (thumbs.medium && thumbs.medium.url) || (thumbs.default && thumbs.default.url) || null;
        found.push({
          id: 'youtube:' + (v.id && v.id.videoId),
          source: 'youtube_search',
          sourceDisplay: 'YouTube \u00b7 ' + (s.channelTitle || 'Search'),
          sourceName: s.channelTitle || 'YouTube',
          title: s.title || '',
          snippet: (s.description || '').substring(0, 300),
          url: 'https://www.youtube.com/watch?v=' + (v.id && v.id.videoId),
          author: s.channelTitle || 'YouTube',
          publishedAt: s.publishedAt || new Date().toISOString(),
          thumbnail: thumb,
          brandTag: m.brand,
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    }
    addMentions('youtube_search', found);
  } catch (err) {
    console.warn(' [MENTIONS] YouTube search failed:', err.message);
  }
}

// --- 5. Local news RSS (Wisconsin outlets — TV, print, radio, college) ---
const LOCAL_NEWS_FEEDS = [
  // Madison TV
  { url: 'https://www.channel3000.com/feed/', name: 'Channel 3000 (WISC-TV)' },
  { url: 'https://www.nbc15.com/arc/outboundfeeds/rss/', name: 'NBC 15 (WMTV)' },
  { url: 'https://www.wkow.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc', name: 'WKOW 27 (ABC Madison)' },
  { url: 'https://fox47.com/feed/', name: 'FOX 47 Madison' },
  // Madison print / magazine
  { url: 'https://madison.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc', name: 'Wisconsin State Journal' },
  { url: 'https://captimes.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc', name: 'Cap Times' },
  { url: 'https://isthmus.com/feed/', name: 'Isthmus' },
  { url: 'https://www.dailycardinal.com/feeds/main.xml', name: 'The Daily Cardinal (UW-Madison)' },
  { url: 'https://badgerherald.com/feed/', name: 'The Badger Herald (UW-Madison)' },
  // Milwaukee
  { url: 'https://www.jsonline.com/rss/', name: 'Milwaukee Journal Sentinel' },
  { url: 'https://urbanmilwaukee.com/feed/', name: 'Urban Milwaukee' },
  { url: 'https://www.tmj4.com/feed', name: 'TMJ4 Milwaukee' },
  { url: 'https://www.cbs58.com/rss', name: 'CBS 58 Milwaukee' },
  { url: 'https://www.fox6now.com/feed', name: 'FOX6 Milwaukee' },
  { url: 'https://www.wisn.com/topstories-rss', name: 'WISN 12 Milwaukee' },
  // Green Bay / Fox Valley
  { url: 'https://www.wbay.com/arc/outboundfeeds/rss/', name: 'WBAY Green Bay' },
  { url: 'https://www.nbc26.com/feed', name: 'NBC 26 Green Bay' },
  { url: 'https://www.wfrv.com/feed/', name: 'WFRV 5 Green Bay' },
  // Statewide / public media / advocacy
  { url: 'https://wisconsinexaminer.com/feed/', name: 'Wisconsin Examiner' },
  { url: 'https://www.wpr.org/rss.xml', name: 'Wisconsin Public Radio' },
  { url: 'https://www.wuwm.com/rss.xml', name: 'WUWM 89.7 (Milwaukee NPR)' },
  { url: 'https://wisbusiness.com/feed/', name: 'WisBusiness' },
  { url: 'https://www.biztimes.com/feed/', name: 'BizTimes Milwaukee' }
];

async function pollLocalNews() {
  const found = [];
  const feedStats = [];
  for (const feed of LOCAL_NEWS_FEEDS) {
    let feedStatus = 'ok';
    let itemsInFeed = 0;
    let matched = 0;
    try {
      const data = await rssParser.parseURL(feed.url);
      itemsInFeed = (data.items || []).length;
      for (const item of (data.items || [])) {
        const titleText = cleanHtml(item.title || '');
        const bodyText = cleanHtml(item.contentSnippet || item.content || '');
        const text = titleText + ' ' + bodyText;
        const m = matchesAnyBrand(text, item.link, titleText);
        if (!m) continue;
        matched++;
        found.push({
          id: 'news:' + (item.guid || item.link),
          source: 'local_news',
          sourceDisplay: feed.name,
          sourceName: feed.name,
          title: cleanHtml(item.title || ''),
          snippet: cleanHtml(item.contentSnippet || '').substring(0, 300),
          url: item.link,
          author: item.creator || feed.name,
          publishedAt: item.isoDate || item.pubDate || new Date().toISOString(),
          thumbnail: imageFromRssItem(item),
          brandTag: m.brand,
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    } catch (err) {
      feedStatus = 'ERROR:' + (err.code || err.message || 'unknown').substring(0, 40);
    }
    feedStats.push(feed.name + ' [' + feedStatus + '] items=' + itemsInFeed + ' matched=' + matched);
  }
  console.log(' [MENTIONS] Local News: ' + LOCAL_NEWS_FEEDS.length + ' feeds, ' + found.length + ' total matches');
  for (const s of feedStats) console.log('   \u2514 ' + s);
  addMentions('local_news', found);
}

// --- 6. Google News RSS (aggregator — broadest single source, covers thousands of outlets) ---
const GOOGLE_NEWS_QUERIES = [
  // Brand mentions
  { q: '"Madison Gas and Electric"', tag: 'mge' },
  { q: '"MG&E" Wisconsin utility', tag: 'mge' },
  { q: '"MGE Energy" Wisconsin', tag: 'mge' },
  // PSC (Wisconsin Public Service Commission) site-scoped — regulatory filings, press releases
  { q: 'site:psc.wi.gov "Madison Gas and Electric"', tag: 'mge' },
  { q: 'site:psc.wi.gov MGE rate', tag: 'mge' },
  { q: '"Madison Gas and Electric" rate case', tag: 'mge' },
  { q: '"Madison Gas and Electric" PSC docket', tag: 'mge' },
  // Competitors
  { q: '"Alliant Energy" Wisconsin', tag: 'alliant' },
  { q: '"We Energies"', tag: 'we_energies' },
  // Topical — Wisconsin energy/utility/renewable industry
  { q: 'Wisconsin utility rate case', tag: 'topical' },
  { q: 'Wisconsin Public Service Commission ruling', tag: 'topical' },
  { q: 'Wisconsin renewable energy', tag: 'topical' },
  { q: 'Wisconsin solar energy', tag: 'topical' },
  { q: 'Madison solar energy', tag: 'topical' },
  { q: 'Wisconsin solar panels', tag: 'topical' },
  { q: 'Wisconsin community solar', tag: 'topical' },
  { q: 'Wisconsin wind energy', tag: 'topical' },
  { q: 'Wisconsin EV charging', tag: 'topical' },
  { q: 'Wisconsin electric vehicle', tag: 'topical' },
  { q: 'Wisconsin battery storage', tag: 'topical' },
  { q: 'Wisconsin grid modernization', tag: 'topical' },
  { q: 'Wisconsin heat pump', tag: 'topical' },
  { q: 'Midwest utility renewable', tag: 'topical' },
  { q: 'Midwest solar energy', tag: 'topical' },
  { q: '"Focus on Energy" Wisconsin', tag: 'topical' }
];

function gNewsRssUrl(q) {
  return 'https://news.google.com/rss/search?q=' + encodeURIComponent(q) + '&hl=en-US&gl=US&ceid=US:en';
}

async function pollGoogleNews() {
  const found = [];
  for (const { q, tag } of GOOGLE_NEWS_QUERIES) {
    try {
      const feed = await rssParser.parseURL(gNewsRssUrl(q));
      for (const item of (feed.items || [])) {
        const titleText = cleanHtml(item.title || '');
        const bodyText = cleanHtml(item.contentSnippet || item.content || '');
        const text = titleText + ' ' + bodyText;
        const m = matchesAnyBrand(text, item.link, titleText);
        if (!m) continue;
        // For topical queries, still require the strict energy signal so we don't drown in noise
        if (tag === 'topical' && m.brand === 'mge') {
          // brand matched — fine, keep
        } else if (tag === 'topical') {
          if (!verifyTopicalContent(text)) continue;
        }
        // Google News source is usually embedded in the title as "Headline - Source Name"
        let srcName = 'Google News';
        const src = item.source && (typeof item.source === 'string' ? item.source : (item.source._ || item.source.name));
        if (src) srcName = String(src);
        else {
          // Fallback: extract from title trailing " - Source"
          const match = (item.title || '').match(/\s-\s([^-]+)$/);
          if (match) srcName = match[1].trim();
        }
        found.push({
          id: 'gnews:' + (item.guid || item.link),
          source: 'google_news',
          sourceDisplay: 'Google News \u00b7 ' + srcName,
          sourceName: srcName,
          title: cleanHtml(item.title || '').replace(/\s-\s[^-]+$/, ''), // strip trailing " - Source"
          snippet: cleanHtml(item.contentSnippet || '').substring(0, 300),
          url: item.link,
          author: srcName,
          publishedAt: item.isoDate || item.pubDate || new Date().toISOString(),
          thumbnail: imageFromRssItem(item),
          brandTag: m.brand,
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    } catch (err) {
      console.warn(' [MENTIONS] Google News query failed (' + q + '):', err.message);
    }
  }
  console.log(' [MENTIONS] Google News: ' + GOOGLE_NEWS_QUERIES.length + ' queries, ' + found.length + ' matches');
  addMentions('google_news', found);
}

// --- 7. Industry trade publications (energy/utility vertical press) ---
const INDUSTRY_FEEDS = [
  { url: 'https://www.utilitydive.com/feeds/news/', name: 'Utility Dive' },
  { url: 'https://www.canarymedia.com/rss/feed', name: 'Canary Media' },
  { url: 'https://energynews.us/feed/', name: 'Energy News Network' },
  { url: 'https://www.renewableenergyworld.com/feed/', name: 'Renewable Energy World' },
  { url: 'https://www.tdworld.com/rss.xml', name: 'T&D World' },
  { url: 'https://www.powermag.com/feed/', name: 'POWER Magazine' }
];

async function pollIndustryNews() {
  const found = [];
  let brandHits = 0, topicalHits = 0;
  for (const feed of INDUSTRY_FEEDS) {
    try {
      const data = await rssParser.parseURL(feed.url);
      for (const item of (data.items || [])) {
        const titleText = cleanHtml(item.title || '');
        const bodyText = cleanHtml(item.contentSnippet || item.content || '');
        const text = titleText + ' ' + bodyText;
        let brandTag, matchedKeyword, confidence;
        // First try brand match (MGE, Alliant, We Energies) — title-aware to kill footer links
        const m = matchesAnyBrand(text, item.link, titleText);
        if (m) {
          brandTag = m.brand;
          matchedKeyword = m.keyword;
          confidence = m.confidence;
          brandHits++;
        } else if (TOPICAL_STRICT_SIGNALS.test(text)) {
          // Industry pubs already pre-filter to utility topics, so accept any strict-signal match
          // (renewables, solar, grid, EVs, decarb) without brand-name requirement
          brandTag = 'topical';
          const sig = text.match(TOPICAL_STRICT_SIGNALS);
          matchedKeyword = sig ? sig[0] : 'industry trend';
          confidence = 'topical';
          topicalHits++;
        } else {
          continue;
        }
        found.push({
          id: 'industry:' + (item.guid || item.link),
          source: 'industry_news',
          sourceDisplay: feed.name,
          sourceName: feed.name,
          title: cleanHtml(item.title || ''),
          snippet: cleanHtml(item.contentSnippet || '').substring(0, 300),
          url: item.link,
          author: item.creator || feed.name,
          publishedAt: item.isoDate || item.pubDate || new Date().toISOString(),
          thumbnail: imageFromRssItem(item),
          brandTag: brandTag,
          matchedKeyword: matchedKeyword,
          confidence: confidence
        });
      }
    } catch (err) {
      console.warn(' [MENTIONS] Industry feed failed: ' + feed.name + ' (' + err.message + ')');
    }
  }
  console.log(' [MENTIONS] Industry News: ' + INDUSTRY_FEEDS.length + ' feeds, ' + found.length + ' matches (' + brandHits + ' brand, ' + topicalHits + ' topical)');
  addMentions('industry_news', found);
}

// --- 8. SEC EDGAR filings (MGE Energy holding co + Madison Gas and Electric subsidiary) ---
// Every 8-K / 10-Q / 10-K / proxy statement shows up here. Atom feed, public domain, no auth.
const SEC_COMPANIES = [
  { cik: '0001141591', name: 'MGE Energy, Inc.', ticker: 'MGEE' }, // holding company (publicly traded)
  { cik: '0000061339', name: 'Madison Gas and Electric Company', ticker: null } // operating utility
];

function secAtomUrl(cik) {
  return 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=' + cik + '&type=&dateb=&owner=include&count=40&output=atom';
}

async function pollSECFilings() {
  const found = [];
  for (const company of SEC_COMPANIES) {
    try {
      const feed = await rssParser.parseURL(secAtomUrl(company.cik));
      for (const item of (feed.items || [])) {
        // Extract form type from title (EDGAR titles are like "8-K - Current Report")
        const formMatch = (item.title || '').match(/^([\dA-Z\-]+)/);
        const formType = formMatch ? formMatch[1] : 'Filing';
        found.push({
          id: 'sec:' + company.cik + ':' + (item.guid || item.link),
          source: 'sec_filings',
          sourceDisplay: 'SEC EDGAR \u00b7 ' + company.name,
          sourceName: company.name + (company.ticker ? ' (' + company.ticker + ')' : ''),
          title: cleanHtml(item.title || ''),
          snippet: cleanHtml(item.contentSnippet || item.content || '').substring(0, 300),
          url: item.link,
          author: company.name,
          publishedAt: item.isoDate || item.pubDate || item.updated || new Date().toISOString(),
          thumbnail: null,
          brandTag: 'mge',
          matchedKeyword: formType,
          confidence: 'high'
        });
      }
    } catch (err) {
      console.warn(' [MENTIONS] SEC EDGAR failed for ' + company.name + ':', err.message);
    }
  }
  console.log(' [MENTIONS] SEC Filings: ' + SEC_COMPANIES.length + ' companies, ' + found.length + ' filings');
  addMentions('sec_filings', found);
}

// --- 9. Apple Podcasts (iTunes Search API — free, no auth) ---
const PODCAST_QUERIES = [
  'Madison Gas and Electric',
  'MGE Energy Wisconsin',
  'Wisconsin utility',
  'Wisconsin energy'
];

async function pollPodcasts() {
  const found = [];
  for (const q of PODCAST_QUERIES) {
    try {
      const url = 'https://itunes.apple.com/search?term=' + encodeURIComponent(q) + '&media=podcast&entity=podcast&limit=15';
      const resp = await fetch(url, { headers: { 'User-Agent': 'MGE-Social-Command-Center/1.0' } });
      if (!resp.ok) continue;
      const data = await resp.json();
      for (const p of (data.results || [])) {
        const title = p.collectionName || p.trackName || '';
        const description = p.description || p.shortDescription || '';
        const text = title + ' ' + description;
        const m = matchesAnyBrand(text, p.collectionViewUrl || p.trackViewUrl);
        if (!m) continue;
        found.push({
          id: 'podcast:' + (p.collectionId || p.trackId || title),
          source: 'podcasts',
          sourceDisplay: 'Apple Podcasts \u00b7 ' + (p.artistName || 'Unknown'),
          sourceName: p.artistName || 'Apple Podcasts',
          title: title,
          snippet: description.substring(0, 300),
          url: p.collectionViewUrl || p.trackViewUrl,
          author: p.artistName || 'Unknown',
          publishedAt: p.releaseDate || new Date().toISOString(),
          thumbnail: p.artworkUrl600 || p.artworkUrl100 || null,
          brandTag: m.brand,
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    } catch (err) {
      console.warn(' [MENTIONS] Podcasts query failed (' + q + '):', err.message);
    }
  }
  console.log(' [MENTIONS] Podcasts: ' + PODCAST_QUERIES.length + ' queries, ' + found.length + ' matches');
  addMentions('podcasts', found);
}

// --- Staggered schedulers (spread API calls, stay well under rate limits) ---
function startMentionPollers() {
  console.log(' [MENTIONS] Starting pollers (9 sources, staggered)...');
  setTimeout(pollReddit, 5000);
  setTimeout(pollLocalNews, 20000);
  setTimeout(pollGoogleNews, 35000);
  setTimeout(pollGoogleAlerts, 50000);
  setTimeout(pollGDELT, 70000);
  setTimeout(pollSECFilings, 85000);
  setTimeout(pollIndustryNews, 105000);
  setTimeout(pollPodcasts, 130000);
  setTimeout(pollYouTubeMentions, 150000);
  setInterval(pollReddit, 30 * 60 * 1000);
  setInterval(pollLocalNews, 30 * 60 * 1000);
  setInterval(pollGoogleNews, 45 * 60 * 1000);
  setInterval(pollGoogleAlerts, 60 * 60 * 1000);
  setInterval(pollGDELT, 120 * 60 * 1000);
  setInterval(pollSECFilings, 360 * 60 * 1000); // SEC filings slow, every 6h
  setInterval(pollIndustryNews, 180 * 60 * 1000);
  setInterval(pollPodcasts, 720 * 60 * 1000); // Podcasts slow, every 12h
  setInterval(pollYouTubeMentions, 240 * 60 * 1000);
}

// --- API endpoints ---
app.get('/api/mentions', (req, res) => {
  const { source, keyword, since, until, brand } = req.query;
  let items = MENTIONS.items.slice();
  if (source && source !== 'all') items = items.filter(m => m.source === source);
  if (brand && brand !== 'all') {
    const brandList = String(brand).split(',').map(s => s.trim()).filter(Boolean);
    if (brandList.length > 0) items = items.filter(m => brandList.includes(m.brandTag || 'mge'));
  }
  if (keyword) {
    const kw = String(keyword).toLowerCase();
    items = items.filter(m => (m.title || '').toLowerCase().includes(kw) || (m.snippet || '').toLowerCase().includes(kw));
  }
  if (since) {
    const sd = new Date(since);
    if (!isNaN(sd.getTime())) items = items.filter(m => new Date(m.publishedAt) >= sd);
  }
  if (until) {
    const ud = new Date(until);
    if (!isNaN(ud.getTime())) items = items.filter(m => new Date(m.publishedAt) <= ud);
  }
  res.json({
    count: items.length,
    total: MENTIONS.items.length,
    lastPoll: MENTIONS.lastPoll,
    stats: MENTIONS.stats,
    brandStats: MENTIONS.brandStats,
    items
  });
});

let _lastManualMentionsRefresh = 0;
app.post('/api/mentions/refresh', (req, res) => {
  const now = Date.now();
  if (now - _lastManualMentionsRefresh < 60000) {
    return res.json({ error: true, message: 'Please wait at least 60 seconds between manual refreshes.' });
  }
  _lastManualMentionsRefresh = now;
  Promise.all([
    pollReddit(),
    pollLocalNews(),
    pollGoogleNews(),
    pollGoogleAlerts(),
    pollGDELT(),
    pollSECFilings(),
    pollIndustryNews(),
    pollPodcasts(),
    pollYouTubeMentions()
  ]).catch(() => {});
  res.json({ ok: true, message: 'Refresh triggered across all 9 sources. New mentions will appear within ~60 seconds.' });
});

if (process.env.NODE_ENV === 'production' || process.env.ENABLE_MENTIONS === 'true') {
  startMentionPollers();
}

// ============================================================
// END MEDIA MONITORING & MENTIONS
// ============================================================


const PORT = config.server.port;
app.listen(PORT, () => {
  console.log('');
  console.log(' +==================================================+');
  console.log(' |  MGE Social Media Command Center v2.0            |');
  console.log(' |  Engagement & Sentiment Monitoring Dashboard     |');
  console.log(' +==================================================+');
  console.log('');
  console.log(` [WEB] Dashboard: http://localhost:${PORT}`);
  if (DASHBOARD_PASSWORD) console.log(` [LOCK] Password protection enabled (use ?pw=<password>)`);
  console.log('');
  console.log(' Platform Status:');
  console.log(`   YouTube    ${config.youtube?.enabled ? '[OK] Configured' : '[ ] Not configured'}`);
  console.log(`   Facebook   ${config.facebook?.enabled ? '[OK] Configured' : '[ ] Not configured'}`);
  console.log(`   Instagram  ${config.instagram?.enabled ? '[OK] Configured' : '[ ] Not configured'}`);
  console.log(`   LinkedIn   ${config.linkedin?.enabled ? '[OK] Configured' : '[ ] Not configured'}`);
  console.log('');
});
