import type { APIRoute } from 'astro';
import { getPopularModels } from '../lib/db';

export const prerender = false;

export const GET: APIRoute = async ({ locals }) => {
  const db = (locals as any).runtime.env.DB;
  const models = await getPopularModels(db, 200);

  // Generate comparison pairs from top models (different makes only)
  const urls: string[] = [];
  const seen = new Set<string>();

  for (let i = 0; i < models.length && urls.length < 2000; i++) {
    for (let j = i + 1; j < models.length && urls.length < 2000; j++) {
      const a = models[i] as any;
      const b = models[j] as any;
      if (a.make_name === b.make_name) continue;
      const slugs = [a.slug, b.slug].sort();
      const key = slugs.join('-vs-');
      if (seen.has(key)) continue;
      seen.add(key);
      urls.push(`https://plaincars.com/compare/${key}`);
    }
  }

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
${urls.map(u => `  <url><loc>${u}</loc><changefreq>monthly</changefreq><priority>0.6</priority></url>`).join('\n')}
</urlset>`;

  return new Response(xml, {
    headers: {
      'Content-Type': 'application/xml',
      'Cache-Control': 'public, max-age=3600, s-maxage=604800',
    },
  });
};
