#!/usr/bin/env node
// Fetch NHTSA safety ratings and update model_years table

import Database from 'better-sqlite3';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = resolve(__dirname, '../data/plaincars.db');
const DELAY = 150;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Get unique make/year combos from our DB where year >= 2011 (NCAP ratings are sparse before that)
  const makeYears = db.prepare(`
    SELECT DISTINCT m.make_name, my.year
    FROM model_years my
    JOIN makes m ON my.make_id = m.make_id
    WHERE my.year >= 2011
    ORDER BY my.year DESC, m.make_name
  `).all();

  console.log('Checking safety ratings for ' + makeYears.length + ' make/year combos');

  const updateRating = db.prepare(`
    UPDATE model_years SET
      overall_rating = ?,
      front_crash_rating = ?,
      side_crash_rating = ?,
      rollover_rating = ?,
      rollover_risk = ?
    WHERE my_id = ?
  `);

  const modelRows = db.prepare('SELECT model_id, make_id, model_name FROM models').all();
  const modelLookup = new Map();
  for (const m of modelRows) {
    modelLookup.set(m.make_id + '|' + m.model_name, m.model_id);
  }

  let updated = 0;
  let apiCalls = 0;

  for (const { make_name, year } of makeYears) {
    try {
      // Step 1: Get models for this make/year
      const url1 = 'https://api.nhtsa.gov/SafetyRatings/modelyear/' + year + '/make/' + encodeURIComponent(make_name);
      const res1 = await fetch(url1);
      if (!res1.ok) continue;
      const data1 = await res1.json();
      apiCalls++;

      if (!data1.Results || data1.Results.length === 0) continue;

      for (const item of data1.Results) {
        const modelName = (item.Model || '').trim().toUpperCase();
        const makeId = item.Make ? item.Make.trim().toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '') : '';
        const modelId = modelLookup.get(makeId + '|' + modelName);
        if (!modelId) continue;
        const myId = modelId + '-' + year;

        // Step 2: Get vehicle variants
        const url2 = 'https://api.nhtsa.gov/SafetyRatings/modelyear/' + year + '/make/' + encodeURIComponent(make_name) + '/model/' + encodeURIComponent(item.Model);
        const res2 = await fetch(url2);
        if (!res2.ok) continue;
        const data2 = await res2.json();
        apiCalls++;

        if (!data2.Results || data2.Results.length === 0) continue;

        // Get the first vehicle with a VehicleId
        const vehicle = data2.Results.find(v => v.VehicleId > 0);
        if (!vehicle) continue;

        // Step 3: Get full ratings
        const url3 = 'https://api.nhtsa.gov/SafetyRatings/VehicleId/' + vehicle.VehicleId;
        const res3 = await fetch(url3);
        if (!res3.ok) continue;
        const data3 = await res3.json();
        apiCalls++;

        if (!data3.Results || data3.Results.length === 0) continue;
        const r = data3.Results[0];

        updateRating.run(
          r.OverallRating || null,
          r.OverallFrontCrashRating || null,
          r.OverallSideCrashRating || null,
          r.RolloverRating || null,
          r.RolloverPossibility || null,
          myId
        );
        updated++;

        await sleep(DELAY);
      }

      if (apiCalls % 50 === 0) console.log('  calls: ' + apiCalls + ', updated: ' + updated);
      await sleep(DELAY);
    } catch (e) { /* skip */ }
  }

  console.log('Done! Updated ' + updated + ' model years with ratings, API calls: ' + apiCalls);
  db.close();
}

main().catch(err => { console.error(err); process.exit(1); });
