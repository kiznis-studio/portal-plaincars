import type { APIRoute } from 'astro';

export const prerender = true;

const pages = [
  { url: '/', priority: '1.0', changefreq: 'weekly' },
  { url: '/make', priority: '0.9', changefreq: 'weekly' },
  { url: '/rankings', priority: '0.8', changefreq: 'weekly' },
  { url: '/recall', priority: '0.8', changefreq: 'daily' },
  { url: '/search', priority: '0.7', changefreq: 'weekly' },
  { url: '/about', priority: '0.4', changefreq: 'monthly' },
  { url: '/privacy', priority: '0.3', changefreq: 'yearly' },
  { url: '/terms', priority: '0.3', changefreq: 'yearly' },
];

export const GET: APIRoute = async () => {
  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${pages.map((p) => `  <url>
    <loc>https://plaincars.com${p.url}</loc>
    <changefreq>${p.changefreq}</changefreq>
    <priority>${p.priority}</priority>
  </url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: { 'Content-Type': 'application/xml' },
  });
};
