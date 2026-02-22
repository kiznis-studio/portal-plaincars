#!/usr/bin/env node
// Upload seed SQL files to Cloudflare D1
// Uses execSync with hardcoded paths only — no user input

import { readdirSync, readFileSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';
import { execSync } from 'child_process';

const __dirname = dirname(fileURLToPath(import.meta.url));
const SEED_DIR = resolve(__dirname, '../data/seed');
const DB_NAME = 'plaincars-db';

const files = readdirSync(SEED_DIR)
  .filter(f => f.endsWith('.sql'))
  .sort();

console.log(`Uploading ${files.length} seed files to D1 (${DB_NAME})...`);

for (const file of files) {
  const path = resolve(SEED_DIR, file);
  const size = readFileSync(path).length;
  console.log(`  ${file} (${(size / 1024).toFixed(1)} KB)...`);

  try {
    execSync(`npx wrangler d1 execute ${DB_NAME} --remote --file="${path}"`, {
      cwd: resolve(__dirname, '..'),
      stdio: 'pipe',
      timeout: 120000,
    });
    console.log(`    ✓ OK`);
  } catch (e) {
    console.error(`    ✗ FAILED: ${e.message.slice(0, 200)}`);
    process.exit(1);
  }
}

console.log('Done! All seed files uploaded.');
