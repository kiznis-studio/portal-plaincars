import { defineConfig } from 'astro/config';
import node from '@astrojs/node';
import tailwindcss from '@tailwindcss/vite';
import sentry from '@sentry/astro';

export default defineConfig({
  output: 'server',
  adapter: node({ mode: 'standalone' }),
  site: 'https://plaincars.com',
  vite: {
    plugins: [tailwindcss()],
  },
  integrations: [
    sentry({
      dsn: 'https://51affb037c96ccbe7ba46605c8ee42df@o4510827630231552.ingest.de.sentry.io/4511031098343504',
      sourceMapsUploadOptions: {
        enabled: false,
      },
    }),
  ],
});
