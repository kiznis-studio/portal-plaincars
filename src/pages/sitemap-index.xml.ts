import type { APIRoute } from 'astro';

export const prerender = false;

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;

  // Get complaint page count for pagination
  const countRow = await db.prepare('SELECT COUNT(*) as cnt FROM complaints').first<{ cnt: number }>();
  const complaintCount = countRow?.cnt || 0;
  const complaintPages = Math.ceil(complaintCount / 50000);

  const sitemaps = [
    'sitemap-static.xml',
    'sitemap-makes.xml',
    'sitemap-models.xml',
    'sitemap-states.xml',
    'sitemap-compare.xml',
    'sitemap-reliability.xml',
    'sitemap-investigations.xml',
    'sitemap-components.xml',
    'sitemap-years.xml',
    ...Array.from({ length: complaintPages }, (_, i) => `sitemap-complaints-${i + 1}.xml`),
  ];

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${sitemaps.map((s) => `  <sitemap><loc>https://plaincars.com/${s}</loc></sitemap>`).join('\n')}
</sitemapindex>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
