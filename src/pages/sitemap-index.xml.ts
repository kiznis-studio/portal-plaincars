import type { APIRoute } from 'astro';

export const prerender = true;

export const GET: APIRoute = async () => {
  const sitemaps = [
    'sitemap-static.xml',
    'sitemap-makes.xml',
    'sitemap-models.xml',
    'sitemap-states.xml',
    'sitemap-compare.xml',
    'sitemap-reliability.xml',
    'sitemap-investigations.xml',
  ];

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${sitemaps.map((s) => `  <sitemap><loc>https://plaincars.com/${s}</loc></sitemap>`).join('\n')}
</sitemapindex>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
