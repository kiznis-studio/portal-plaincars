import type { APIRoute } from 'astro';
import { getAllComponents } from '../lib/db';

export const prerender = false;

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;
  const components = await getAllComponents(db);

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>https://plaincars.com/component</loc>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>
${components.map((c) => `  <url>
    <loc>https://plaincars.com/component/${c.component_slug}</loc>
    <changefreq>monthly</changefreq>
    <priority>0.6</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
