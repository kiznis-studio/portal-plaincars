import { defineMiddleware } from 'astro:middleware';
import { existsSync } from 'node:fs';
import { isbot } from 'isbot';
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

// In-memory rate limiter — protects all endpoints from scraper/unknown bot abuse
// Known bots (Googlebot, GPTBot, ClaudeBot, etc.) bypass rate limiting via isbot library
const rateLimitMap = new Map<string, { count: number; resetAt: number }>();
const PAGE_RATE_LIMIT = 60; // page requests per minute per IP
const API_RATE_LIMIT = 30; // API requests per minute per IP
const RATE_WINDOW = 60_000; // 1 minute

function isRateLimited(ip: string, limit: number): boolean {
  const now = Date.now();
  const entry = rateLimitMap.get(ip);

  if (!entry || now > entry.resetAt) {
    rateLimitMap.set(ip, { count: 1, resetAt: now + RATE_WINDOW });
    return false;
  }

  entry.count++;
  return entry.count > limit;
}

function cleanupRateLimits() {
  const now = Date.now();
  if (rateLimitMap.size > 1000) {
    for (const [ip, entry] of rateLimitMap) {
      if (now > entry.resetAt) rateLimitMap.delete(ip);
    }
  }
}

// In-memory response cache for rendered pages (LRU-style with TTL)
const responseCache = new Map<string, { body: string; headers: Record<string, string>; expiry: number }>();
const CACHE_TTL = 300_000; // 5 minutes
const MAX_CACHE_ENTRIES = 500;

function getCachedResponse(key: string): Response | null {
  const entry = responseCache.get(key);
  if (!entry || Date.now() > entry.expiry) {
    if (entry) responseCache.delete(key);
    return null;
  }
  return new Response(entry.body, {
    headers: { ...entry.headers, 'X-Cache': 'HIT' },
  });
}

function cacheResponse(key: string, body: string, headers: Record<string, string>) {
  if (responseCache.size >= MAX_CACHE_ENTRIES) {
    // Evict oldest entry
    const firstKey = responseCache.keys().next().value;
    if (firstKey) responseCache.delete(firstKey);
  }
  responseCache.set(key, { body, headers, expiry: Date.now() + CACHE_TTL });
}

function getClientIp(request: Request): string {
  return request.headers.get('cf-connecting-ip')
    || request.headers.get('x-forwarded-for')?.split(',')[0]?.trim()
    || 'unknown';
}

export const onRequest = defineMiddleware(async (context, next) => {
  const path = context.url.pathname;

  // Inject D1-compatible DB adapter into locals
  (context.locals as any).runtime = { env: { DB: getDb() } };

  // Skip rate limiting and caching for static assets
  if (path.startsWith('/_astro/') || path.startsWith('/favicon')) {
    return next();
  }

  const ip = getClientIp(context.request);
  const ua = context.request.headers.get('user-agent') || '';
  cleanupRateLimits();

  // Rate limit unknown traffic only — let search engines and AI bots crawl freely
  if (!isbot(ua)) {
    const limit = path.startsWith('/api/') ? API_RATE_LIMIT : PAGE_RATE_LIMIT;
    if (isRateLimited(ip, limit)) {
      return new Response('Too many requests', {
        status: 429,
        headers: { 'Retry-After': '60', 'Cache-Control': 'no-store' },
      });
    }
  }

  // Serve from in-memory cache for GET requests (bots hit the same pages repeatedly)
  if (context.request.method === 'GET') {
    const cacheKey = path + context.url.search;
    const cached = getCachedResponse(cacheKey);
    if (cached) return cached;

    const response = await next();

    // Cache successful HTML/XML responses
    if (response.status === 200) {
      const ct = response.headers.get('content-type') || '';
      if (ct.includes('text/html') || ct.includes('xml')) {
        const ttl = ct.includes('xml') ? 86400 : 3600;
        const body = await response.text();
        const headers: Record<string, string> = {
          'Content-Type': ct,
          'Cache-Control': `public, max-age=300, s-maxage=${ttl}`,
        };
        cacheResponse(cacheKey, body, headers);
        return new Response(body, { headers: { ...headers, 'X-Cache': 'MISS' } });
      }
    }

    return response;
  }

  return next();
});
