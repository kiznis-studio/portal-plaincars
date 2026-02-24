import type { APIRoute } from 'astro';
import { STATE_MAP } from '../lib/db';

export const prerender = false;

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  // Get states that actually have complaint data
  const { results } = await db.prepare(
    `SELECT DISTINCT state FROM complaints WHERE state IS NOT NULL AND state != ''`
  ).all<{ state: string }>();

  const validStates = results
    .filter((r: any) => STATE_MAP[r.state])
    .map((r: any) => STATE_MAP[r.state].slug);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://plaincars.com/states</loc>
    <changefreq>weekly</changefreq>
    <priority>0.8</priority>
  </url>
${validStates.map((slug: string) => `  <url>
    <loc>https://plaincars.com/states/${slug}</loc>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
