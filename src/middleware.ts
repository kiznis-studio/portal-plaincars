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
  // Only rate-limit API endpoints (SSR routes, never prerendered)
  if (context.url.pathname.startsWith('/api/')) {
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
  }

  return next();
});
