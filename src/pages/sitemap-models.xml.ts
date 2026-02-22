import type { APIRoute } from 'astro';

export const prerender = false;

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;
  const models = await db.prepare(
    `SELECT m.slug as model_slug, mk.slug as make_slug
     FROM models m JOIN makes mk ON m.make_id = mk.make_id
     ORDER BY m.complaint_count DESC`
  ).all();

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${models.results.map((m: any) => `  <url>
    <loc>https://plaincars.com/make/${m.make_slug}/${m.model_slug}</loc>
    <changefreq>weekly</changefreq>
    <priority>0.7</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
