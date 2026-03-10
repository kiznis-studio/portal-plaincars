#!/usr/bin/env node
/**
 * merge-vin-cache.mjs — Merge API-cached VIN decodes into vin-decoder.db
 *
 * The PlainCars VIN decoder falls back to the NHTSA API when local decode
 * fails, and caches the result in a writable SQLite DB (vin-api-cache.db).
 * This script merges those cached entries back into the main vin-decoder.db
 * so future decodes are fully local.
 *
 * Usage:
 *   node scripts/merge-vin-cache.mjs <vin-decoder.db> <vin-api-cache.db>
 *
 * What it does:
 *   1. Reads all entries from vin_cache in the API cache DB
 *   2. For each entry, inserts into api_patterns table in vin-decoder.db
 *   3. Reports stats: merged, skipped (already local), errors
 *
 * After merging:
 *   - VACUUM the vin-decoder.db
 *   - Upload to Titan: scp vin-decoder.db root@titan:/opt/portals/data/sqlite/
 *   - Optionally clear the cache: rm vin-api-cache.db (it will be recreated)
 */

import Database from 'better-sqlite3';
import { existsSync } from 'node:fs';

const args = process.argv.slice(2);
if (args.length < 2) {
  console.error('Usage: node scripts/merge-vin-cache.mjs <vin-decoder.db> <vin-api-cache.db>');
  process.exit(1);
}

const [vinDbPath, cachePath] = args;

if (!existsSync(vinDbPath)) {
  console.error(`ERROR: vin-decoder.db not found at ${vinDbPath}`);
  process.exit(1);
}
if (!existsSync(cachePath)) {
  console.error(`ERROR: vin-api-cache.db not found at ${cachePath}`);
  process.exit(1);
}

const cacheDb = new Database(cachePath, { readonly: true });
const vinDb = new Database(vinDbPath);

// Create api_patterns table for API-sourced decodes, keyed by descriptor + year.
vinDb.exec(`
  CREATE TABLE IF NOT EXISTS api_patterns (
    descriptor TEXT NOT NULL,
    year TEXT NOT NULL,
    make TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    body_class TEXT NOT NULL DEFAULT '',
    drive_type TEXT NOT NULL DEFAULT '',
    engine_cylinders TEXT NOT NULL DEFAULT '',
    displacement_l TEXT NOT NULL DEFAULT '',
    fuel_type TEXT NOT NULL DEFAULT '',
    plant_city TEXT NOT NULL DEFAULT '',
    plant_state TEXT NOT NULL DEFAULT '',
    plant_country TEXT NOT NULL DEFAULT '',
    vehicle_type TEXT NOT NULL DEFAULT '',
    gvwr TEXT NOT NULL DEFAULT '',
    manufacturer TEXT NOT NULL DEFAULT '',
    source_hit_count INTEGER NOT NULL DEFAULT 0,
    merged_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (descriptor, year)
  );
`);

const cacheEntries = cacheDb.prepare('SELECT * FROM vin_cache').all();
console.log(`Found ${cacheEntries.length} entries in API cache`);

const insertStmt = vinDb.prepare(`
  INSERT OR REPLACE INTO api_patterns
    (descriptor, year, make, model, body_class, drive_type, engine_cylinders,
     displacement_l, fuel_type, plant_city, plant_state, plant_country,
     vehicle_type, gvwr, manufacturer, source_hit_count)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
`);

const checkExisting = vinDb.prepare(
  'SELECT 1 FROM api_patterns WHERE descriptor = ? AND year = ?'
);

let merged = 0;
let updated = 0;
let skipped = 0;
let errors = 0;

const mergeAll = vinDb.transaction(() => {
  for (const entry of cacheEntries) {
    try {
      if (!entry.make || !entry.model) {
        skipped++;
        continue;
      }

      const existing = checkExisting.get(entry.descriptor, entry.year);

      insertStmt.run(
        entry.descriptor, entry.year, entry.make, entry.model,
        entry.body_class, entry.drive_type, entry.engine_cylinders,
        entry.displacement_l, entry.fuel_type, entry.plant_city,
        entry.plant_state, entry.plant_country, entry.vehicle_type,
        entry.gvwr, entry.manufacturer, entry.hit_count || 0
      );

      if (existing) {
        updated++;
      } else {
        merged++;
      }
    } catch (err) {
      errors++;
      console.warn(`  Error merging ${entry.descriptor}/${entry.year}:`, err.message);
    }
  }
});

mergeAll();

const totalApiPatterns = vinDb.prepare('SELECT COUNT(*) as cnt FROM api_patterns').get();

console.log(`\nMerge complete:`);
console.log(`  New:     ${merged}`);
console.log(`  Updated: ${updated}`);
console.log(`  Skipped: ${skipped} (empty make/model)`);
console.log(`  Errors:  ${errors}`);
console.log(`  Total api_patterns: ${totalApiPatterns.cnt}`);
console.log(`\nNext steps:`);
console.log(`  1. Run: sqlite3 ${vinDbPath} "ANALYZE; VACUUM; PRAGMA journal_mode=DELETE;"`);
console.log(`  2. Upload: scp ${vinDbPath} root@178.156.163.82:/opt/portals/data/sqlite/vin-decoder.db`);
console.log(`  3. Restart container on Titan`);

cacheDb.close();
vinDb.close();
