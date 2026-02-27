import type { APIRoute } from 'astro';

export const prerender = false;

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  const { results } = await db.prepare(
    'SELECT nhtsa_id FROM investigations ORDER BY open_date DESC'
  ).all<{ nhtsa_id: string }>();

  const urls = [
    { loc: 'https://plaincars.com/investigations/', priority: '0.8' },
    ...results.map((r) => ({
      loc: 'https://plaincars.com/investigations/' + r.nhtsa_id,
      priority: '0.5',
    })),
  ];

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map((u) => `  <url><loc>${u.loc}</loc><priority>${u.priority}</priority></url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
