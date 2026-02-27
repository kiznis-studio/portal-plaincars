#!/usr/bin/env node
// Fetch NHTSA investigations and match to our makes/models
// API: https://api.nhtsa.gov/investigations (paginated, no auth)

import Database from 'better-sqlite3';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = resolve(__dirname, '../data/plaincars.db');
const DELAY = 150;
const PAGE_SIZE = 50;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function main() {
  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');

  // Ensure investigations table exists
  db.exec(`
    CREATE TABLE IF NOT EXISTS investigations (
      nhtsa_id TEXT PRIMARY KEY,
      subject TEXT NOT NULL,
      investigation_type TEXT NOT NULL,
      status TEXT NOT NULL,
      open_date TEXT,
      latest_activity_date TEXT,
      description TEXT,
      make_id TEXT,
      model_id TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_inv_make ON investigations(make_id);
    CREATE INDEX IF NOT EXISTS idx_inv_model ON investigations(model_id);
    CREATE INDEX IF NOT EXISTS idx_inv_status ON investigations(status);
    CREATE INDEX IF NOT EXISTS idx_inv_type ON investigations(investigation_type);
  `);

  // Load makes and models for matching
  const makes = db.prepare('SELECT make_id, make_name FROM makes').all();
  const models = db.prepare('SELECT model_id, make_id, model_name FROM models').all();

  // Build lookup maps (uppercase name -> id)
  const makeByName = new Map();
  for (const m of makes) {
    makeByName.set(m.make_name.toUpperCase(), m.make_id);
  }
  // Sort by name length DESC so longer names match first (e.g., "LAND ROVER" before "ROVER")
  const sortedMakeNames = [...makeByName.keys()].sort((a, b) => b.length - a.length);

  const modelsByMake = new Map();
  for (const m of models) {
    if (!modelsByMake.has(m.make_id)) modelsByMake.set(m.make_id, []);
    modelsByMake.get(m.make_id).push({ model_id: m.model_id, model_name: m.model_name.toUpperCase() });
  }

  // Match investigation text to our makes/models
  function matchMakeModel(text) {
    var upper = text.toUpperCase();
    var matchedMakeId = null;
    var matchedModelId = null;

    // Try to find a make name in the text
    for (var i = 0; i < sortedMakeNames.length; i++) {
      var makeName = sortedMakeNames[i];
      // Require word boundary (space, start, punctuation before/after)
      var escaped = makeName.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      var regex = new RegExp('(?:^|[\\s,;:(])' + escaped + '(?:[\\s,;:).!]|$)');
      if (regex.test(upper)) {
        matchedMakeId = makeByName.get(makeName);
        // Now try to find a model name for this make
        var modelsForMake = modelsByMake.get(matchedMakeId) || [];
        // Sort by model_name length DESC for longest match first
        var sorted = modelsForMake.slice().sort(function(a, b) { return b.model_name.length - a.model_name.length; });
        for (var j = 0; j < sorted.length; j++) {
          var model = sorted[j];
          if (model.model_name.length < 2) continue;
          var mEscaped = model.model_name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
          var mRegex = new RegExp('(?:^|[\\s,;:(])' + mEscaped + '(?:[\\s,;:).!]|$)');
          if (mRegex.test(upper)) {
            matchedModelId = model.model_id;
            break;
          }
        }
        break;
      }
    }
    return { make_id: matchedMakeId, model_id: matchedModelId };
  }

  // Strip HTML tags from description
  function stripHtml(html) {
    return (html || '').replace(/<[^>]*>/g, ' ').replace(/&nbsp;/g, ' ').replace(/\s+/g, ' ').trim();
  }

  // Fetch all investigations with pagination
  var insertInv = db.prepare(
    'INSERT OR REPLACE INTO investigations (nhtsa_id, subject, investigation_type, status, open_date, latest_activity_date, description, make_id, model_id) VALUES (?,?,?,?,?,?,?,?,?)'
  );

  var offset = 0;
  var total = 0;
  var fetched = 0;
  var matched = 0;

  // First request to get total
  var firstUrl = 'https://api.nhtsa.gov/investigations?max=' + PAGE_SIZE + '&offset=0&sort=id&order=desc';
  var firstRes = await fetch(firstUrl);
  var firstData = await firstRes.json();
  total = firstData.meta.pagination.total;
  console.log('Total investigations: ' + total);

  // Process a batch of investigation results
  function processBatch(results) {
    db.transaction(function() {
      for (var k = 0; k < results.length; k++) {
        var inv = results[k];
        var plainDesc = stripHtml(inv.description);
        var textToMatch = (inv.subject || '') + ' ' + plainDesc.slice(0, 500);
        var match = matchMakeModel(textToMatch);
        if (match.make_id) matched++;

        insertInv.run(
          inv.nhtsaId || '',
          (inv.subject || '').slice(0, 500),
          inv.investigationType || '',
          inv.status === 'O' ? 'O' : 'C',
          inv.openDate ? inv.openDate.slice(0, 10) : null,
          inv.latestActivityDate ? inv.latestActivityDate.slice(0, 10) : null,
          plainDesc.slice(0, 2000),
          match.make_id,
          match.model_id
        );
        fetched++;
      }
    })();
  }

  processBatch(firstData.results);
  offset += PAGE_SIZE;
  console.log('  fetched: ' + fetched + ', matched: ' + matched);

  // Fetch remaining pages
  while (offset < total) {
    await sleep(DELAY);
    try {
      var url = 'https://api.nhtsa.gov/investigations?max=' + PAGE_SIZE + '&offset=' + offset + '&sort=id&order=desc';
      var res = await fetch(url);
      if (!res.ok) {
        console.error('  HTTP ' + res.status + ' at offset ' + offset);
        offset += PAGE_SIZE;
        continue;
      }
      var data = await res.json();
      if (!data.results || data.results.length === 0) break;
      processBatch(data.results);
      offset += PAGE_SIZE;
      if (fetched % 500 === 0) console.log('  fetched: ' + fetched + '/' + total + ', matched: ' + matched);
    } catch (e) {
      console.error('  Error at offset ' + offset + ': ' + e.message);
      offset += PAGE_SIZE;
    }
  }

  console.log('Done! Fetched: ' + fetched + '/' + total + ', Matched to vehicles: ' + matched);
  console.log('  Open: ' + db.prepare('SELECT COUNT(*) as c FROM investigations WHERE status = ?').get('O').c);
  console.log('  Closed: ' + db.prepare('SELECT COUNT(*) as c FROM investigations WHERE status = ?').get('C').c);
  console.log('  With make: ' + db.prepare('SELECT COUNT(*) as c FROM investigations WHERE make_id IS NOT NULL').get().c);
  console.log('  With model: ' + db.prepare('SELECT COUNT(*) as c FROM investigations WHERE model_id IS NOT NULL').get().c);

  db.close();
}

main().catch(function(err) { console.error(err); process.exit(1); });
