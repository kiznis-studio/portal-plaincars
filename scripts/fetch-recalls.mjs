#!/usr/bin/env node
// Fetch NHTSA recalls for all make/model/year combos in our DB

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

  // Get unique make/model/year combos for years >= 2000
  const combos = db.prepare(`
    SELECT DISTINCT mk.make_name, mo.model_name, my.year, mk.make_id, mo.model_id, my.my_id
    FROM model_years my
    JOIN makes mk ON my.make_id = mk.make_id
    JOIN models mo ON my.model_id = mo.model_id
    WHERE my.year >= 2000
    ORDER BY mk.make_name, mo.model_name, my.year DESC
  `).all();

  console.log('Fetching recalls for ' + combos.length + ' make/model/year combos');

  const insertRecall = db.prepare(
    'INSERT OR IGNORE INTO recalls (recall_id, campaign_number, my_id, make_id, model_id, year, component, summary, consequence, remedy, report_date, affected_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
  );

  let totalRecalls = 0;
  let apiCalls = 0;

  for (const c of combos) {
    try {
      const url = 'https://api.nhtsa.gov/recalls/recallsByVehicle?make=' +
        encodeURIComponent(c.make_name) + '&model=' +
        encodeURIComponent(c.model_name) + '&modelYear=' + c.year;
      const res = await fetch(url);
      if (!res.ok) { await sleep(DELAY); continue; }
      const data = await res.json();
      apiCalls++;

      if (data.results && data.results.length > 0) {
        db.transaction(() => {
          for (const r of data.results) {
            const recallId = (r.NHTSACampaignNumber || '') + '-' + c.make_id + '-' + c.model_id + '-' + c.year;
            insertRecall.run(
              recallId,
              r.NHTSACampaignNumber || '',
              c.my_id,
              c.make_id,
              c.model_id,
              c.year,
              (r.Component || '').slice(0, 200),
              (r.Summary || '').slice(0, 1000),
              (r.Consequence || '').slice(0, 500),
              (r.Remedy || '').slice(0, 500),
              r.ReportReceivedDate || '',
              null
            );
            totalRecalls++;
          }
        })();
      }
      if (apiCalls % 200 === 0) console.log('  calls: ' + apiCalls + ', recalls: ' + totalRecalls);
      await sleep(DELAY);
    } catch (e) { await sleep(DELAY); }
  }

  // Update recall counts on model_years, models, makes
  console.log('Updating recall counts...');
  db.exec('UPDATE model_years SET recall_count = (SELECT COUNT(DISTINCT campaign_number) FROM recalls WHERE recalls.my_id = model_years.my_id)');
  db.exec('UPDATE models SET recall_count = (SELECT COUNT(DISTINCT campaign_number) FROM recalls WHERE recalls.model_id = models.model_id)');
  db.exec('UPDATE makes SET recall_count = (SELECT COUNT(DISTINCT campaign_number) FROM recalls WHERE recalls.make_id = makes.make_id)');

  console.log('Done! Total recalls: ' + totalRecalls + ', API calls: ' + apiCalls);
  db.close();
}

main().catch(err => { console.error(err); process.exit(1); });
