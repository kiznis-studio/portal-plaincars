import { defineMiddleware } from 'astro:middleware';
import { existsSync } from 'node:fs';
import { isbot } from 'isbot';
import { createD1Adapter } from './lib/d1-adapter';

// --- DB initialization (single-DB template — multi-DB portals customize this section) ---
const DATABASE_PATH = process.env.DATABASE_PATH || '/data/portal.db';
let db: ReturnType<typeof createD1Adapter> | null = null;
function getDb() {
  if (!db) {
    if (!existsSync(DATABASE_PATH)) return null as any; // build time: no DB file
    db = createD1Adapter(DATABASE_PATH);
  }
  return db;
}

// --- Concurrency guard ---
// Hard cap on simultaneous inflight requests. Returns 503 if exceeded.
// Cloudflare handles DDoS/bot protection at the edge (authenticated origin pull).
let inflightRequests = 0;
const MAX_CONCURRENT = 15;

// --- Event loop lag tracking ---
let eventLoopLag = 0;
const lagInterval = setInterval(() => {
  const start = performance.now();
  setImmediate(() => { eventLoopLag = performance.now() - start; });
}, 1000);
lagInterval.unref();

// --- In-memory response cache (5min TTL, 500 entries) ---
const responseCache = new Map<string, { body: string; headers: Record<string, string>; expiry: number }>();
const CACHE_TTL = 300_000;
const MAX_CACHE_ENTRIES = 500;

// Interval-based cleanup instead of per-request cleanup
const cleanupInterval = setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of responseCache) {
    if (now > entry.expiry) responseCache.delete(key);
  }
}, 600_000); // every 10 minutes
cleanupInterval.unref();

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
    const firstKey = responseCache.keys().next().value;
    if (firstKey) responseCache.delete(firstKey);
  }
  responseCache.set(key, { body, headers, expiry: Date.now() + CACHE_TTL });
}

// Exported for health endpoint
export { inflightRequests, eventLoopLag, responseCache };

export const onRequest = defineMiddleware(async (context, next) => {
  const path = context.url.pathname;

  // Inject D1-compatible DB adapter into locals
  (context.locals as any).runtime = { env: { DB: getDb() } };

  // Health endpoint bypasses all middleware logic
  if (path === '/health') return next();

  // Skip for static assets
  if (path.startsWith('/_astro/') || path.startsWith('/favicon')) return next();

  // Serve from in-memory cache for GET requests
  if (context.request.method === 'GET') {
    const cacheKey = path + context.url.search;
    const cached = getCachedResponse(cacheKey);
    if (cached) return cached;

    // Concurrency guard — known bots (Googlebot, etc.) bypass since they're the traffic we want
    const ua = context.request.headers.get('user-agent') || '';
    const isBotUA = isbot(ua);
    if (!isBotUA && inflightRequests >= MAX_CONCURRENT) {
      return new Response('Service busy', {
        status: 503,
        headers: { 'Retry-After': '5', 'Cache-Control': 'no-store' },
      });
    }

    if (!isBotUA) inflightRequests++;
    const start = performance.now();
    try {
      const response = await next();

      // Slow request logging
      const elapsed = performance.now() - start;
      if (elapsed > 500) {
        console.warn(`[slow] ${path} ${Math.round(elapsed)}ms lag=${Math.round(eventLoopLag)}ms`);
      }

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
    } finally {
      if (!isBotUA) inflightRequests--;
    }
  }

  return next();
});
