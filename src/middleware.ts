import { defineMiddleware } from 'astro:middleware';
import { existsSync } from 'node:fs';
import { createD1Adapter } from './lib/d1-adapter';

// Initialize DB adapter once (persistent across requests — Node.js long-lived process)
const DATABASE_PATH = process.env.DATABASE_PATH || '/data/portal.db';
let db: ReturnType<typeof createD1Adapter> | null = null;
function getDb() {
  if (!db) {
    if (!existsSync(DATABASE_PATH)) return null as any; // build time: no DB file
    db = createD1Adapter(DATABASE_PATH);
  }
  return db;
}

// Simple in-memory rate limiter for API endpoints
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

  // Inject D1-compatible DB adapter into locals (same path as Cloudflare runtime)
  // This means all page code accessing Astro.locals.runtime.env.DB works unchanged
  (context.locals as any).runtime = { env: { DB: getDb() } };

  // Rate-limit API endpoints only
  if (path.startsWith('/api/')) {
    const ip = context.request.headers.get('x-forwarded-for')?.split(',')[0]?.trim()
      || context.request.headers.get('cf-connecting-ip')
      || 'unknown';

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

  const response = await next();

  // Set Cache-Control headers for HTML/XML responses
  // Cloudflare CDN (in front) will respect s-maxage for edge caching
  if (response.status === 200 && context.request.method === 'GET') {
    const ct = response.headers.get('content-type') || '';
    if (ct.includes('text/html') || ct.includes('xml')) {
      const ttl = ct.includes('xml') ? 86400 : 3600;
      const newResponse = new Response(response.body, response);
      newResponse.headers.set('Cache-Control', `public, max-age=300, s-maxage=${ttl}`);
      return newResponse;
    }
  }

  return response;
});
