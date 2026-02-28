import { defineMiddleware } from 'astro:middleware';

// Simple in-memory rate limiter for API endpoints
// Cloudflare Workers isolates are short-lived, so this resets naturally
// This protects against burst abuse (rapid-fire requests from one IP)
const rateLimitMap = new Map<string, { count: number; resetAt: number }>();
const RATE_LIMIT = 30; // requests per window
const RATE_WINDOW = 60_000; // 1 minute in ms

function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);

  if (!entry || now > entry.resetAt) {
    rateLimitMap.set(ip, { count: 1, resetAt: now + RATE_WINDOW });
    return false;
  }

  entry.count++;
  if (entry.count > RATE_LIMIT) {
    return true;
  }
  return false;
}

// Cleanup stale entries periodically (prevent memory leak in long-lived isolates)
function cleanupRateLimits() {
  const now = Date.now();
  if (rateLimitMap.size > 1000) {
    for (const [ip, entry] of rateLimitMap) {
      if (now > entry.resetAt) rateLimitMap.delete(ip);
    }
  }
}

export const onRequest = defineMiddleware(async (context, next) => {
  const path = context.url.pathname;

  // Rate-limit API endpoints only
  if (path.startsWith('/api/')) {
    let ip = 'unknown';
    try { ip = context.clientAddress || context.request.headers.get('cf-connecting-ip') || 'unknown'; } catch { ip = context.request.headers.get('cf-connecting-ip') || 'unknown'; }

    cleanupRateLimits();

    if (isRateLimited(ip)) {
      return new Response(JSON.stringify({ error: 'Too many requests' }), {
        status: 429,
        headers: {
          'Content-Type': 'application/json',
          'Retry-After': '60',
          'Cache-Control': 'no-store',
        },
      });
    }

    return next();
  }

  // --- Cloudflare Cache API for SSR pages ---
  // Workers bypass the CDN cache by default. We must explicitly use the Cache API
  // to store/retrieve responses at Cloudflare's edge, preventing D1 reads from crawlers.
  const cache = typeof caches !== 'undefined' ? caches.default : null;
  const cacheKey = new Request(context.url.toString(), { method: 'GET' });

  if (cache && context.request.method === 'GET') {
    const cached = await cache.match(cacheKey);
    if (cached) return cached;
  }

  const response = await next();

  // Only cache successful HTML/XML responses
  if (cache && response.status === 200 && context.request.method === 'GET') {
    const ct = response.headers.get('content-type') || '';
    if (ct.includes('text/html') || ct.includes('xml')) {
      const ttl = ct.includes('xml') ? 86400 : 21600; // 24h sitemaps, 6h HTML
      const cacheResponse = new Response(response.body, response);
      cacheResponse.headers.set('Cache-Control', `public, max-age=300, s-maxage=${ttl}`);
      // waitUntil keeps the Worker alive to complete the cache.put() in the background
      const waitUntil = (context.locals as any).runtime?.waitUntil;
      if (waitUntil) {
        waitUntil(cache.put(cacheKey, cacheResponse.clone()));
      } else {
        await cache.put(cacheKey, cacheResponse.clone());
      }
      return cacheResponse;
    }
  }

  return response;
});
