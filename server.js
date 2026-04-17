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
const BRAND_EXACT = [
  /\bMadison Gas and Electric\b/i,
  /\bMadison Gas & Electric\b/i,
  /\bMG&E\b/i,
  /\bMGE Energy\b/i
];
const BRAND_AMBIGUOUS = /\bMGE\b/;
const UTILITY_CONTEXT = /\b(outage|power|electric|gas|utility|bill|rate|customer|energy|meter|grid|blackout|restoration|substation|transformer|kilowatt|kwh)\b/i;
const WISCONSIN_GEO = /\b(Wisconsin|Madison|Dane County|Sun Prairie|Middleton|Fitchburg|Monona|Verona|Waunakee|Stoughton|Cottage Grove|McFarland)\b/i;
const NEGATIVE_FILTERS = /\bMGE Wealth|MGE Group|Mitsubishi|MGE Capital\b/i;

function matchesBrand(text) {
  if (!text) return null;
  if (NEGATIVE_FILTERS.test(text)) return null;
  for (const p of BRAND_EXACT) {
    const m = text.match(p);
    if (m) return { keyword: m[0], confidence: 'high' };
  }
  if (BRAND_AMBIGUOUS.test(text)) {
    const u = UTILITY_CONTEXT.test(text);
    const g = WISCONSIN_GEO.test(text);
    if (u || g) return { keyword: 'MGE', confidence: (u && g) ? 'high' : 'medium' };
  }
  return null;
}

// --- In-memory cache (no DB, intentional for free-tier footprint) ---
const MENTIONS = {
  items: [],
  lastPoll: {},
  stats: { reddit: 0, google_alerts: 0, gdelt: 0, youtube_search: 0, local_news: 0 },
  maxSize: 500
};

function addMentions(source, incoming) {
  MENTIONS.lastPoll[source] = new Date().toISOString();
  if (!incoming || incoming.length === 0) return 0;
  // Filter out job postings/careers listings
  const beforeJob = incoming.length;
  incoming = incoming.filter(m => !isJobListing(m));
  const jobSkipped = beforeJob - incoming.length;
  if (jobSkipped > 0) console.log(' [MENTIONS] ' + source + ': filtered ' + jobSkipped + ' job listing(s)');
  const existing = new Set(MENTIONS.items.map(m => m.id));
  const additions = incoming.filter(i => !existing.has(i.id));
  if (additions.length === 0) return 0;
  MENTIONS.items = [...additions, ...MENTIONS.items]
    .sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt))
    .slice(0, MENTIONS.maxSize);
  MENTIONS.stats[source] = (MENTIONS.stats[source] || 0) + additions.length;
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
async function pollReddit() {
  try {
    const queries = [
      'https://www.reddit.com/r/madisonwi+wisconsin+madison+greenbay+milwaukee/search.json?q=%22Madison+Gas+and+Electric%22+OR+%22MG%26E%22+OR+MGE&restrict_sr=1&sort=new&limit=25&t=week',
      'https://www.reddit.com/search.json?q=%22Madison+Gas+and+Electric%22+OR+%22MG%26E%22&sort=new&limit=25&t=week'
    ];
    const found = [];
    for (const url of queries) {
      const resp = await fetch(url, { headers: { 'User-Agent': 'MGE-Social-Command-Center/1.0' } });
      if (!resp.ok) continue;
      const data = await resp.json();
      for (const c of (data && data.data && data.data.children) || []) {
        const d = c.data;
        const text = (d.title || '') + ' ' + (d.selftext || '');
        const m = matchesBrand(text);
        if (!m) continue;
        // Reddit thumbnail extraction
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
        found.push({
          id: 'reddit:' + d.id,
          source: 'reddit',
          sourceDisplay: 'Reddit \u00b7 r/' + d.subreddit,
          sourceName: 'r/' + d.subreddit,
          title: d.title || '',
          snippet: (d.selftext || '').substring(0, 300),
          url: 'https://www.reddit.com' + d.permalink,
          author: 'u/' + (d.author || 'unknown'),
          publishedAt: new Date((d.created_utc || Date.now() / 1000) * 1000).toISOString(),
          thumbnail: thumb,
          matchedKeyword: m.keyword,
          confidence: m.confidence,
          engagement: { score: d.score || 0, comments: d.num_comments || 0 }
        });
      }
    }
    addMentions('reddit', found);
  } catch (err) {
    console.warn(' [MENTIONS] Reddit failed:', err.message);
  }
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
        const text = cleanHtml(item.title || '') + ' ' + cleanHtml(item.contentSnippet || item.content || '');
        const m = matchesBrand(text);
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
    const queries = ['"Madison Gas and Electric"', '"Madison Gas & Electric"', '"MG&E" Wisconsin'];
    const found = [];
    for (const q of queries) {
      const url = `https://api.gdeltproject.org/api/v2/doc/doc?query=${encodeURIComponent(q)}&mode=ArtList&format=json&maxrecords=25&sort=DateDesc&timespan=7d`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const body = await resp.text();
      let data; try { data = JSON.parse(body); } catch { continue; }
      for (const a of (data.articles || [])) {
        const m = matchesBrand(a.title || '');
        if (!m) continue;
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
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    }
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
    const queries = ['"Madison Gas and Electric"', '"MG&E" Wisconsin'];
    const found = [];
    for (const q of queries) {
      const url = `https://www.googleapis.com/youtube/v3/search?part=snippet&q=${encodeURIComponent(q)}&type=video&order=date&maxResults=15&key=${apiKey}`;
      const resp = await fetch(url);
      if (!resp.ok) continue;
      const data = await resp.json();
      for (const v of (data.items || [])) {
        const s = v.snippet || {};
        const m = matchesBrand((s.title || '') + ' ' + (s.description || ''));
        if (!m) continue;
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

// --- 5. Local news RSS ---
const LOCAL_NEWS_FEEDS = [
  { url: 'https://www.channel3000.com/feed/', name: 'Channel 3000' },
  { url: 'https://www.nbc15.com/arc/outboundfeeds/rss/', name: 'NBC 15' },
  { url: 'https://madison.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc', name: 'Wisconsin State Journal' },
  { url: 'https://isthmus.com/feed/', name: 'Isthmus' },
  { url: 'https://captimes.com/search/?f=rss&t=article&l=50&s=start_time&sd=desc', name: 'Cap Times' },
  { url: 'https://www.jsonline.com/rss/', name: 'Milwaukee Journal Sentinel' }
];

async function pollLocalNews() {
  const found = [];
  for (const feed of LOCAL_NEWS_FEEDS) {
    try {
      const data = await rssParser.parseURL(feed.url);
      for (const item of (data.items || [])) {
        const text = cleanHtml(item.title || '') + ' ' + cleanHtml(item.contentSnippet || item.content || '');
        const m = matchesBrand(text);
        if (!m) continue;
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
          matchedKeyword: m.keyword,
          confidence: m.confidence
        });
      }
    } catch (err) {
      console.warn(' [MENTIONS] RSS feed failed: ' + feed.name + ' (' + err.message + ')');
    }
  }
  addMentions('local_news', found);
}

// --- Staggered schedulers (spread API calls, stay well under rate limits) ---
function startMentionPollers() {
  console.log(' [MENTIONS] Starting pollers (5 sources, staggered)...');
  setTimeout(pollReddit, 5000);
  setTimeout(pollLocalNews, 20000);
  setTimeout(pollGoogleAlerts, 35000);
  setTimeout(pollGDELT, 60000);
  setTimeout(pollYouTubeMentions, 90000);
  setInterval(pollReddit, 30 * 60 * 1000);
  setInterval(pollLocalNews, 30 * 60 * 1000);
  setInterval(pollGoogleAlerts, 60 * 60 * 1000);
  setInterval(pollGDELT, 120 * 60 * 1000);
  setInterval(pollYouTubeMentions, 240 * 60 * 1000);
}

// --- API endpoints ---
app.get('/api/mentions', (req, res) => {
  const { source, keyword, since } = req.query;
  let items = MENTIONS.items.slice();
  if (source && source !== 'all') items = items.filter(m => m.source === source);
  if (keyword) {
    const kw = String(keyword).toLowerCase();
    items = items.filter(m => (m.title || '').toLowerCase().includes(kw) || (m.snippet || '').toLowerCase().includes(kw));
  }
  if (since) {
    const sd = new Date(since);
    if (!isNaN(sd.getTime())) items = items.filter(m => new Date(m.publishedAt) >= sd);
  }
  res.json({ count: items.length, total: MENTIONS.items.length, lastPoll: MENTIONS.lastPoll, stats: MENTIONS.stats, items });
});

let _lastManualMentionsRefresh = 0;
app.post('/api/mentions/refresh', (req, res) => {
  const now = Date.now();
  if (now - _lastManualMentionsRefresh < 60000) {
    return res.json({ error: true, message: 'Please wait at least 60 seconds between manual refreshes.' });
  }
  _lastManualMentionsRefresh = now;
  Promise.all([pollReddit(), pollLocalNews(), pollGoogleAlerts()]).catch(() => {});
  res.json({ ok: true, message: 'Refresh triggered. New mentions will appear within ~15 seconds.' });
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
