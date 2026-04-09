/**
 * MGE Social Media Command Center - Backend Server
 * Proxies API calls to Facebook, Instagram, LinkedIn, and YouTube
 * Serves the dashboard and handles token management
 *
 * Supports environment variables for cloud deployment with config.json fallback
 */

const express = require('express');
const path = require('path');
const fs = require('fs');

const app = express();
app.use(express.json());

// Ã¢ÂÂÃ¢ÂÂ Config Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
// Load config from environment variables with config.json fallback
function loadConfig() {
  const fileConfig = (() => {
    try {
      return JSON.parse(fs.readFileSync(path.join(__dirname, 'config.json'), 'utf8'));
    } catch (e) {
      return null;
    }
  })();

  // Merge environment variables (take precedence) with file config (fallback)
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

  // Log config source for debugging
  if (process.env.NODE_ENV !== 'production') {
    const usingEnv = !!(process.env.YOUTUBE_API_KEY || process.env.FACEBOOK_PAGE_TOKEN ||
                        process.env.INSTAGRAM_TOKEN || process.env.LINKEDIN_TOKEN);
    console.log(`  Config source: ${usingEnv ? 'Environment variables' : 'config.json (or defaults)'}`);
  }

  return config;
}

let config = loadConfig();

// Reload config on each request so credential changes take effect without restart
app.use((req, res, next) => {
  if (req.path.startsWith('/api')) config = loadConfig();
  next();
});

// Ã¢ÂÂÃ¢ÂÂ Password Protection (Optional) Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
// If DASHBOARD_PASSWORD env var is set, require it as query param or show login
const DASHBOARD_PASSWORD = process.env.DASHBOARD_PASSWORD;

if (DASHBOARD_PASSWORD) {
  app.use((req, res, next) => {
    // Don't protect static assets, API status, or the root route for now
    if (req.path === '/' || req.path === '/index.html' ||
        req.path.endsWith('.css') || req.path.endsWith('.js') ||
        req.path.endsWith('.html')) {

      const passwordParam = req.query.pw;
      const passwordCookie = req.cookies?.dashboardAuth;

      if (!passwordParam && !passwordCookie) {
        // Return login page
        return res.send(`
          <!DOCTYPE html>
          <html>
          <head>
            <title>MGE Social Command Center - Login</title>
            <style>
              body { font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; margin: 0; background: #f5f5f5; }
              .login-box { background: white; padding: 40px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); width: 100%; max-width: 300px; }
              h1 { margin-top: 0; color: #333; text-align: center; }
              input { width: 100%; padding: 10px; margin: 10px 0; border: 1px solid #ddd; border-radius: 4px; font-size: 16px; box-sizing: border-box; }
              button { width: 100%; padding: 10px; background: #007bff; color: white; border: none; border-radius: 4px; cursor: pointer; font-size: 16px; }
              button:hover { background: #0056b3; }
            </style>
          </head>
          <body>
            <div class="login-box">
              <h1>MGE Social Command Center</h1>
              <form method="GET" action="/">
                <input type="password" name="pw" placeholder="Enter dashboard password" required autofocus>
                <button type="submit">Login</button>
              </form>
            </div>
          </body>
          </html>
        `);
      }

      if ((passwordParam || passwordCookie) && (passwordParam !== DASHBOARD_PASSWORD && passwordCookie !== DASHBOARD_PASSWORD)) {
        return res.status(403).send('Incorrect password');
      }
    }
    next();
  });
}

// Ã¢ÂÂÃ¢ÂÂ Serve Dashboard Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
app.use(express.static(__dirname));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'MGE_Social_Command_Center.html')));

// Ã¢ÂÂÃ¢ÂÂ Helper: safe fetch with error handling Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
async function apiFetch(url, options = {}) {
  try {
    const resp = await fetch(url, options);
    if (!resp.ok) {
      const text = await resp.text();
      return { error: true, status: resp.status, message: text };
    }
    return await resp.json();
  } catch (err) {
    return { error: true, message: err.message };
  }
}

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  API STATUS
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
app.get('/api/status', (req, res) => {
  res.json({
    youtube: !!(config.youtube?.enabled && config.youtube?.apiKey && config.youtube?.channelId),
    facebook: !!(config.facebook?.enabled && config.facebook?.pageAccessToken && config.facebook?.pageId),
    instagram: !!(config.instagram?.enabled && config.instagram?.accessToken && config.instagram?.igUserId),
    linkedin: !!(config.linkedin?.enabled && config.linkedin?.accessToken && config.linkedin?.organizationId),
    refreshInterval: config.server?.refreshIntervalSeconds || 3600
  });
});

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  YOUTUBE API ROUTES
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
const YT_BASE = 'https://www.googleapis.com/youtube/v3';

app.get('/api/youtube/channel', async (req, res) => {
  const { apiKey, channelId } = config.youtube || {};
  if (!apiKey || !channelId) return res.json({ error: true, message: 'YouTube not configured' });

  const data = await apiFetch(
    `${YT_BASE}/channels?part=snippet,statistics,brandingSettings&id=${channelId}&key=${apiKey}`
  );
  res.json(data);
});

app.get('/api/youtube/videos', async (req, res) => {
  const { apiKey, channelId } = config.youtube || {};
  if (!apiKey || !channelId) return res.json({ error: true, message: 'YouTube not configured' });

  // First get upload playlist
  const channelData = await apiFetch(
    `${YT_BASE}/channels?part=contentDetails&id=${channelId}&key=${apiKey}`
  );
  if (channelData.error) return res.json(channelData);

  const uploadsId = channelData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  if (!uploadsId) return res.json({ error: true, message: 'No uploads playlist found' });

  // Get recent videos from playlist
  const playlist = await apiFetch(
    `${YT_BASE}/playlistItems?part=snippet&playlistId=${uploadsId}&maxResults=20&key=${apiKey}`
  );
  if (playlist.error) return res.json(playlist);

  const videoIds = playlist.items?.map(i => i.snippet.resourceId.videoId).join(',');
  if (!videoIds) return res.json({ items: [] });

  // Get video stats
  const videos = await apiFetch(
    `${YT_BASE}/videos?part=snippet,statistics&id=${videoIds}&key=${apiKey}`
  );
  res.json(videos);
});

app.get('/api/youtube/comments/:videoId', async (req, res) => {
  const { apiKey } = config.youtube || {};
  if (!apiKey) return res.json({ error: true, message: 'YouTube not configured' });

  const data = await apiFetch(
    `${YT_BASE}/commentThreads?part=snippet&videoId=${req.params.videoId}&maxResults=50&order=time&key=${apiKey}`
  );
  res.json(data);
});

// Get comments across all recent videos
app.get('/api/youtube/all-comments', async (req, res) => {
  const { apiKey, channelId } = config.youtube || {};
  if (!apiKey || !channelId) return res.json({ error: true, message: 'YouTube not configured' });

  // Get recent videos first
  const channelData = await apiFetch(
    `${YT_BASE}/channels?part=contentDetails&id=${channelId}&key=${apiKey}`
  );
  if (channelData.error) return res.json(channelData);

  const uploadsId = channelData.items?.[0]?.contentDetails?.relatedPlaylists?.uploads;
  const playlist = await apiFetch(
    `${YT_BASE}/playlistItems?part=snippet&playlistId=${uploadsId}&maxResults=5&key=${apiKey}`
  );
  if (playlist.error) return res.json(playlist);

  const videoIds = playlist.items?.map(i => i.snippet.resourceId.videoId) || [];

  // Fetch comments for each video in parallel
  const commentPromises = videoIds.map(vid =>
    apiFetch(`${YT_BASE}/commentThreads?part=snippet&videoId=${vid}&maxResults=20&order=time&key=${apiKey}`)
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

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  FACEBOOK (META GRAPH API) ROUTES
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
const META_BASE = 'https://graph.facebook.com/v22.0';

app.get('/api/facebook/page', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });

  const data = await apiFetch(
    `${META_BASE}/${pageId}?fields=id,name,fan_count,followers_count,picture,about,engagement&access_token=${pageAccessToken}`
  );
  res.json(data);
});

app.get('/api/facebook/posts', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });

  const data = await apiFetch(
    `${META_BASE}/${pageId}/posts?fields=id,message,created_time,full_picture,permalink_url,shares,reactions.summary(true),comments.summary(true),likes.summary(true)&limit=20&access_token=${pageAccessToken}`
  );
  res.json(data);
});

app.get('/api/facebook/comments/:postId', async (req, res) => {
  const { pageAccessToken } = config.facebook || {};
  if (!pageAccessToken) return res.json({ error: true, message: 'Facebook not configured' });

  const data = await apiFetch(
    `${META_BASE}/${req.params.postId}/comments?fields=id,message,created_time,from,like_count,comment_count&limit=50&order=reverse_chronological&access_token=${pageAccessToken}`
  );
  res.json(data);
});

app.get('/api/facebook/all-comments', async (req, res) => {
  const { pageAccessToken, pageId } = config.facebook || {};
  if (!pageAccessToken || !pageId) return res.json({ error: true, message: 'Facebook not configured' });

  // Get recent posts
  const postsData = await apiFetch(
    `${META_BASE}/${pageId}/posts?fields=id,message,created_time,comments{message,created_time,from,like_count}&limit=10&access_token=${pageAccessToken}`
  );
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
        postMessage: post.message || ''
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
  const data = await apiFetch(
    `${META_BASE}/${pageId}/insights?metric=${metrics}&period=day&date_preset=last_30d&access_token=${pageAccessToken}`
  );
  res.json(data);
});

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  INSTAGRAM (META GRAPH API) ROUTES
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
app.get('/api/instagram/profile', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });

  const data = await apiFetch(
    `${META_BASE}/${igUserId}?fields=id,name,username,profile_picture_url,followers_count,follows_count,media_count,biography&access_token=${accessToken}`
  );
  res.json(data);
});

app.get('/api/instagram/media', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });

  const data = await apiFetch(
    `${META_BASE}/${igUserId}/media?fields=id,caption,media_type,media_url,thumbnail_url,permalink,timestamp,like_count,comments_count&limit=20&access_token=${accessToken}`
  );
  res.json(data);
});

app.get('/api/instagram/comments/:mediaId', async (req, res) => {
  const { accessToken } = config.instagram || {};
  if (!accessToken) return res.json({ error: true, message: 'Instagram not configured' });

  const data = await apiFetch(
    `${META_BASE}/${req.params.mediaId}/comments?fields=id,text,timestamp,username,like_count&limit=50&access_token=${accessToken}`
  );
  res.json(data);
});

app.get('/api/instagram/all-comments', async (req, res) => {
  const { accessToken, igUserId } = config.instagram || {};
  if (!accessToken || !igUserId) return res.json({ error: true, message: 'Instagram not configured' });

  const mediaData = await apiFetch(
    `${META_BASE}/${igUserId}/media?fields=id,caption,timestamp,comments{text,timestamp,username,like_count}&limit=10&access_token=${accessToken}`
  );
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
        postCaption: post.caption || ''
      });
    });
  });

  allComments.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
  res.json({ comments: allComments });
});

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  LINKEDIN API ROUTES
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
const LI_BASE = 'https://api.linkedin.com/v2';
const LI_REST = 'https://api.linkedin.com/rest';

app.get('/api/linkedin/organization', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });

  const data = await apiFetch(
    `${LI_BASE}/organizations/${organizationId}?projection=(id,localizedName,vanityName,logoV2,followersCount)`, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'LinkedIn-Version': '202503',
        'X-Restli-Protocol-Version': '2.0.0'
      }
    }
  );
  res.json(data);
});

app.get('/api/linkedin/follower-count', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });

  const data = await apiFetch(
    `${LI_BASE}/organizationalEntityFollowerStatistics?q=organizationalEntity&organizationalEntity=urn:li:organization:${organizationId}`, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'LinkedIn-Version': '202503',
        'X-Restli-Protocol-Version': '2.0.0'
      }
    }
  );
  res.json(data);
});

app.get('/api/linkedin/posts', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });

  const data = await apiFetch(
    `${LI_REST}/posts?author=urn:li:organization:${organizationId}&q=author&count=20&sortBy=LAST_MODIFIED`, {
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'LinkedIn-Version': '202503'
      }
    }
  );
  res.json(data);
});

app.get('/api/linkedin/social-actions/:postUrn', async (req, res) => {
  const { accessToken } = config.linkedin || {};
  if (!accessToken) return res.json({ error: true, message: 'LinkedIn not configured' });

  const urn = decodeURIComponent(req.params.postUrn);
  const [likes, comments] = await Promise.all([
    apiFetch(`${LI_REST}/socialActions/${encodeURIComponent(urn)}/likes?count=50`, {
      headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' }
    }),
    apiFetch(`${LI_REST}/socialActions/${encodeURIComponent(urn)}/comments?count=50`, {
      headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' }
    })
  ]);
  res.json({ likes, comments });
});

app.get('/api/linkedin/all-comments', async (req, res) => {
  const { accessToken, organizationId } = config.linkedin || {};
  if (!accessToken || !organizationId) return res.json({ error: true, message: 'LinkedIn not configured' });

  // Get recent posts
  const postsData = await apiFetch(
    `${LI_REST}/posts?author=urn:li:organization:${organizationId}&q=author&count=10&sortBy=LAST_MODIFIED`, {
      headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' }
    }
  );
  if (postsData.error) return res.json(postsData);

  const posts = postsData.elements || [];
  const commentPromises = posts.map(p =>
    apiFetch(`${LI_REST}/socialActions/${encodeURIComponent(p.id)}/comments?count=20`, {
      headers: { 'Authorization': `Bearer ${accessToken}`, 'LinkedIn-Version': '202503' }
    })
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

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  AGGREGATE ENDPOINTS
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
app.get('/api/all-data', async (req, res) => {
  const status = {
    youtube: !!(config.youtube?.enabled && config.youtube?.apiKey),
    facebook: !!(config.facebook?.enabled && config.facebook?.pageAccessToken),
    instagram: !!(config.instagram?.enabled && config.instagram?.accessToken),
    linkedin: !!(config.linkedin?.enabled && config.linkedin?.accessToken)
  };

  const fetches = {};

  if (status.youtube) {
    fetches.youtubeChannel = apiFetch(`http://localhost:${config.server?.port || 3000}/api/youtube/channel`);
    fetches.youtubeVideos = apiFetch(`http://localhost:${config.server?.port || 3000}/api/youtube/videos`);
    fetches.youtubeComments = apiFetch(`http://localhost:${config.server?.port || 3000}/api/youtube/all-comments`);
  }
  if (status.facebook) {
    fetches.facebookPage = apiFetch(`http://localhost:${config.server?.port || 3000}/api/facebook/page`);
    fetches.facebookPosts = apiFetch(`http://localhost:${config.server?.port || 3000}/api/facebook/posts`);
    fetches.facebookComments = apiFetch(`http://localhost:${config.server?.port || 3000}/api/facebook/all-comments`);
  }
  if (status.instagram) {
    fetches.instagramProfile = apiFetch(`http://localhost:${config.server?.port || 3000}/api/instagram/profile`);
    fetches.instagramMedia = apiFetch(`http://localhost:${config.server?.port || 3000}/api/instagram/media`);
    fetches.instagramComments = apiFetch(`http://localhost:${config.server?.port || 3000}/api/instagram/all-comments`);
  }
  if (status.linkedin) {
    fetches.linkedinOrg = apiFetch(`http://localhost:${config.server?.port || 3000}/api/linkedin/organization`);
    fetches.linkedinPosts = apiFetch(`http://localhost:${config.server?.port || 3000}/api/linkedin/posts`);
    fetches.linkedinComments = apiFetch(`http://localhost:${config.server?.port || 3000}/api/linkedin/all-comments`);
  }

  const keys = Object.keys(fetches);
  const values = await Promise.all(Object.values(fetches));
  const result = { status };
  keys.forEach((k, i) => result[k] = values[i]);

  res.json(result);
});

// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
//  START SERVER
// Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ
const PORT = config.server.port;
app.listen(PORT, () => {
  console.log('');
  console.log('  Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ');
  console.log('  Ã¢ÂÂ   MGE Social Media Command Center v2.0          Ã¢ÂÂ');
  console.log('  Ã¢ÂÂ   Engagement & Sentiment Monitoring Dashboard    Ã¢ÂÂ');
  console.log('  Ã¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂÃ¢ÂÂ');
  console.log('');
  console.log(`  Ã°ÂÂÂ Dashboard:  http://localhost:${PORT}`);
  if (DASHBOARD_PASSWORD) {
    console.log(`  Ã°ÂÂÂ Password protection enabled (use ?pw=<password>)`);
  }
  console.log('');
  console.log('  Platform Status:');
  console.log(`    YouTube   ${config.youtube?.enabled ? 'Ã¢ÂÂ Configured' : 'Ã¢Â¬Â  Not configured'}`);
  console.log(`    Facebook  ${config.facebook?.enabled ? 'Ã¢ÂÂ Configured' : 'Ã¢Â¬Â  Not configured'}`);
  console.log(`    Instagram ${config.instagram?.enabled ? 'Ã¢ÂÂ Configured' : 'Ã¢Â¬Â  Not configured'}`);
  console.log(`    LinkedIn  ${config.linkedin?.enabled ? 'Ã¢ÂÂ Configured' : 'Ã¢Â¬Â  Not configured'}`);
  console.log('');
  console.log('  Configuration:');
  console.log('    - Use environment variables for cloud deployment');
  console.log('    - Falls back to config.json for local development');
  console.log('    - Changes take effect on next data refresh (no restart needed)');
  console.log('');
});
