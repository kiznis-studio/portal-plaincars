import type { APIRoute } from 'astro';

export const prerender = false;

const PAGE_SIZE = 50000;

export const GET: APIRoute = async ({ params, locals }) => {
  const db = (locals as any).runtime.env.DB;
  const page = parseInt(params.page || '1');
  if (page < 1) return new Response('Not found', { status: 404 });

  const offset = (page - 1) * PAGE_SIZE;
  const { results } = await db.prepare(
    'SELECT cmplid FROM complaints ORDER BY cmplid LIMIT ? OFFSET ?'
  ).bind(PAGE_SIZE, offset).all<{ cmplid: number }>();

  if (results.length === 0) return new Response('Not found', { status: 404 });

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${results.map((c) => `  <url>
    <loc>https://plaincars.com/complaint/${c.cmplid}</loc>
    <changefreq>yearly</changefreq>
    <priority>0.4</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
