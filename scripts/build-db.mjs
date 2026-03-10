#!/usr/bin/env node
// Parse NHTSA complaints flat file into local SQLite DB
// Input: /storage/plaincars/FLAT_CMPL.txt (tab-delimited, 2.18M rows)
// Output: data/plaincars.db
// Mega-Grow: stores ALL complaints (no year filter), extracts VIN, builds materialized tables

import Database from 'better-sqlite3';
import { readFileSync, createReadStream } from 'fs';
import { createInterface } from 'readline';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = resolve(__dirname, '../data/plaincars.db');
const SCHEMA_PATH = resolve(__dirname, '../data/schema.sql');
const CMPL_PATH = '/storage/plaincars/FLAT_CMPL.txt';

function slugify(text) {
  return text.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
}

const MAJOR_MAKES = new Set([
  'ACURA','ALFA ROMEO','ASTON MARTIN','AUDI','BENTLEY','BMW','BUICK','CADILLAC',
  'CHEVROLET','CHRYSLER','DODGE','FERRARI','FIAT','FORD','GENESIS','GMC','HONDA',
  'HYUNDAI','INFINITI','JAGUAR','JEEP','KIA','LAMBORGHINI','LAND ROVER','LEXUS',
  'LINCOLN','LOTUS','MASERATI','MAZDA','MCLAREN','MERCEDES-BENZ','MERCURY','MINI',
  'MITSUBISHI','NISSAN','OLDSMOBILE','PLYMOUTH','PONTIAC','PORSCHE','RAM','RIVIAN',
  'ROLLS-ROYCE','SAAB','SATURN','SCION','SMART','SUBARU','SUZUKI','TESLA','TOYOTA',
  'VOLKSWAGEN','VOLVO','LUCID','POLESTAR','VINFAST','FISKER','HUMMER','ISUZU',
  'DAEWOO','DAIHATSU','GEO','EAGLE','AMC','DELOREAN','FREIGHTLINER','INTERNATIONAL',
  'KENWORTH','MACK','PETERBILT','STERLING','WESTERN STAR','WORKHORSE',
  'MERCEDES BENZ','ALFA','LAND ROVER/RANGE ROVER',
]);

function normalizeMake(raw) {
  const upper = raw.trim().toUpperCase();
  if (upper === 'MERCEDES BENZ') return 'MERCEDES-BENZ';
  if (upper === 'ALFA') return 'ALFA ROMEO';
  if (upper.includes('LAND ROVER')) return 'LAND ROVER';
  return upper;
}

async function main() {
  console.log('Building PlainCars database...');

  const db = new Database(DB_PATH);
  db.pragma('journal_mode = WAL');
  db.pragma('synchronous = OFF');
  const schema = readFileSync(SCHEMA_PATH, 'utf8');
  db.exec(schema);

  // --- Phase 1: Parse complaints file ---
  console.log('Phase 1: Parsing complaints...');

  const makeMap = new Map();
  const modelMap = new Map();
  const myMap = new Map();
  const csMap = new Map();
  const complaints = [];

  let lineCount = 0;
  let skipped = 0;

  const rl = createInterface({
    input: createReadStream(CMPL_PATH, { encoding: 'latin1' }),
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    lineCount++;
    if (lineCount % 500000 === 0) console.log('  ' + lineCount + ' lines...');

    const fields = line.split('\t');
    if (fields.length < 20) { skipped++; continue; }

    const cmplid = parseInt(fields[0]) || 0;
    const odiNumber = parseInt(fields[1]) || 0;
    const makeRaw = (fields[3] || '').trim().toUpperCase();
    const modelRaw = (fields[4] || '').trim().toUpperCase();
    const yearRaw = parseInt(fields[5]) || 0;
    const crash = (fields[6] || '').trim();
    const fire = (fields[8] || '').trim();
    const injured = parseInt(fields[9]) || 0;
    const deaths = parseInt(fields[10]) || 0;
    const component = (fields[11] || '').trim();
    const state = (fields[13] || '').trim();
    const dateAdded = (fields[15] || '').trim();
    const vin = (fields[14] || '').trim();
    const mileage = parseInt(fields[17]) || null;
    const summary = (fields[19] || '').trim();
    const prodType = (fields[45] || '').trim();

    if (prodType && prodType !== 'V') { skipped++; continue; }
    if (yearRaw < 1970 || yearRaw > 2027 || yearRaw === 9999) { skipped++; continue; }

    const makeName = normalizeMake(makeRaw);
    if (!makeName || makeName.length < 2) { skipped++; continue; }
    if (!modelRaw || modelRaw.length < 1) { skipped++; continue; }
    if (!MAJOR_MAKES.has(makeName)) { skipped++; continue; }

    const makeKey = makeName;
    const modelKey = makeName + '|' + modelRaw;
    const myKey = makeName + '|' + modelRaw + '|' + yearRaw;
    const csKey = myKey + '|' + component;

    if (!makeMap.has(makeKey)) {
      makeMap.set(makeKey, { name: makeName, complaint_count: 0, models: new Set() });
    }
    const make = makeMap.get(makeKey);
    make.complaint_count++;
    make.models.add(modelRaw);

    if (!modelMap.has(modelKey)) {
      modelMap.set(modelKey, { make: makeName, model: modelRaw, years: new Set(), complaint_count: 0 });
    }
    const model = modelMap.get(modelKey);
    model.years.add(yearRaw);
    model.complaint_count++;

    if (!myMap.has(myKey)) {
      myMap.set(myKey, {
        make: makeName, model: modelRaw, year: yearRaw,
        complaint_count: 0, crash_count: 0, fire_count: 0,
        injury_count: 0, death_count: 0,
      });
    }
    const my = myMap.get(myKey);
    my.complaint_count++;
    if (crash === 'Y') my.crash_count++;
    if (fire === 'Y') my.fire_count++;
    my.injury_count += injured;
    my.death_count += deaths;

    if (component) {
      if (!csMap.has(csKey)) {
        csMap.set(csKey, {
          myKey, component,
          complaint_count: 0, crash_count: 0, fire_count: 0,
          injury_count: 0, death_count: 0, sample_text: '',
        });
      }
      const cs = csMap.get(csKey);
      cs.complaint_count++;
      if (crash === 'Y') cs.crash_count++;
      if (fire === 'Y') cs.fire_count++;
      cs.injury_count += injured;
      cs.death_count += deaths;
      if (!cs.sample_text && summary.length > 20) {
        cs.sample_text = summary.slice(0, 300);
      }
    }

    // Store ALL individual complaints (mega-grow: no year filter)
    complaints.push({
      cmplid, odiNumber, myKey, vin,
      crash, fire, injured, deaths,
      component, summary: summary.slice(0, 1000),
      fail_date: (fields[7] || '').trim(),
      date_added: dateAdded,
      mileage, state,
    });
  }

  console.log('  Parsed ' + lineCount + ' lines, skipped ' + skipped);
  console.log('  Makes: ' + makeMap.size + ', Models: ' + modelMap.size + ', ModelYears: ' + myMap.size);
  console.log('  Component stats: ' + csMap.size + ', Individual complaints: ' + complaints.length);

  // --- Phase 2: Insert into SQLite ---
  console.log('Phase 2: Inserting into SQLite...');

  const insertMake = db.prepare(
    'INSERT OR REPLACE INTO makes (make_id, make_name, slug, complaint_count, model_count) VALUES (?,?,?,?,?)'
  );
  const insertModel = db.prepare(
    'INSERT OR REPLACE INTO models (model_id, make_id, model_name, slug, year_min, year_max, complaint_count) VALUES (?,?,?,?,?,?,?)'
  );
  const insertMY = db.prepare(
    'INSERT OR REPLACE INTO model_years (my_id, model_id, make_id, year, complaint_count, crash_count, fire_count, injury_count, death_count) VALUES (?,?,?,?,?,?,?,?,?)'
  );
  const insertCS = db.prepare(
    'INSERT OR REPLACE INTO complaint_stats (my_id, component, complaint_count, crash_count, fire_count, injury_count, death_count, sample_text) VALUES (?,?,?,?,?,?,?,?)'
  );
  const insertComplaint = db.prepare(
    'INSERT OR REPLACE INTO complaints (cmplid, my_id, odi_number, vin, crash, fire, injured, deaths, component, summary, fail_date, date_added, mileage, state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
  );

  function buildMyId(makeName, modelName, year) {
    return slugify(makeName) + '-' + slugify(modelName) + '-' + year;
  }

  // Insert makes
  db.transaction(() => {
    for (const [name, data] of makeMap) {
      const id = slugify(name);
      insertMake.run(id, name, id, data.complaint_count, data.models.size);
    }
  })();
  console.log('  Inserted ' + makeMap.size + ' makes');

  // Insert models
  db.transaction(() => {
    for (const [key, data] of modelMap) {
      const makeId = slugify(data.make);
      const modelSlug = makeId + '-' + slugify(data.model);
      const years = [...data.years].sort((a, b) => a - b);
      insertModel.run(modelSlug, makeId, data.model, modelSlug, years[0], years[years.length - 1], data.complaint_count);
    }
  })();
  console.log('  Inserted ' + modelMap.size + ' models');

  // Insert model years
  db.transaction(() => {
    for (const [key, data] of myMap) {
      const myId = buildMyId(data.make, data.model, data.year);
      const makeId = slugify(data.make);
      const modelId = makeId + '-' + slugify(data.model);
      insertMY.run(myId, modelId, makeId, data.year, data.complaint_count, data.crash_count, data.fire_count, data.injury_count, data.death_count);
    }
  })();
  console.log('  Inserted ' + myMap.size + ' model years');

  // Insert complaint stats
  let csCount = 0;
  db.transaction(() => {
    for (const [key, data] of csMap) {
      const parts = key.split('|');
      const myId = buildMyId(parts[0], parts[1], parts[2]);
      insertCS.run(myId, data.component, data.complaint_count, data.crash_count, data.fire_count, data.injury_count, data.death_count, data.sample_text);
      csCount++;
    }
  })();
  console.log('  Inserted ' + csCount + ' complaint stat rows');

  // Insert individual complaints in batches
  const BATCH_SIZE = 10000;
  let complaintInserted = 0;
  for (let i = 0; i < complaints.length; i += BATCH_SIZE) {
    const batch = complaints.slice(i, i + BATCH_SIZE);
    db.transaction(() => {
      for (const c of batch) {
        const parts = c.myKey.split('|');
        const myId = buildMyId(parts[0], parts[1], parts[2]);
        insertComplaint.run(c.cmplid, myId, c.odiNumber, c.vin || null, c.crash, c.fire, c.injured, c.deaths, c.component, c.summary, c.fail_date, c.date_added, c.mileage, c.state);
      }
    })();
    complaintInserted += batch.length;
    if (complaintInserted % 200000 === 0) console.log('  Complaints: ' + complaintInserted + '...');
  }
  console.log('  Inserted ' + complaintInserted + ' individual complaints');

  // Pre-compute _stats for D1 efficiency
  console.log('Pre-computing _stats...');
  db.prepare('CREATE TABLE IF NOT EXISTS _stats (key TEXT PRIMARY KEY, value TEXT NOT NULL)').run();

  const ns = db.prepare(`SELECT
    (SELECT COUNT(*) FROM makes) as total_makes,
    (SELECT COUNT(*) FROM models) as total_models,
    (SELECT COUNT(*) FROM model_years) as total_model_years,
    (SELECT COUNT(*) FROM complaints) as total_complaints,
    (SELECT COUNT(*) FROM recalls) as total_recalls,
    (SELECT SUM(death_count) FROM model_years) as total_deaths,
    (SELECT SUM(injury_count) FROM model_years) as total_injuries,
    (SELECT AVG(complaint_count) FROM model_years WHERE complaint_count > 0) as avg_complaints_per_model,
    (SELECT AVG(overall_rating) FROM model_years WHERE overall_rating IS NOT NULL) as avg_rating,
    (SELECT COUNT(*) FROM model_years WHERE overall_rating IS NOT NULL) as models_with_rating,
    (SELECT COUNT(DISTINCT vin) FROM complaints WHERE vin IS NOT NULL AND vin != '') as total_vins
  `).get();
  db.prepare("INSERT OR REPLACE INTO _stats VALUES ('national_stats', ?)").run(JSON.stringify(ns));

  const sc = db.prepare(`SELECT state, COUNT(*) as complaint_count FROM complaints WHERE state IS NOT NULL AND state != '' GROUP BY state ORDER BY complaint_count DESC`).all();
  db.prepare("INSERT OR REPLACE INTO _stats VALUES ('state_complaints', ?)").run(JSON.stringify(sc));

  console.log('  _stats: national_stats, state_complaints (' + sc.length + ' states)');

  // --- Phase 3: Materialized Tables ---
  console.log('Phase 3: Building materialized tables...');

  // Complaint trends by model x calendar year (extracted from date_added)
  console.log('  Building complaint_trends...');
  db.prepare('DELETE FROM complaint_trends').run();
  db.prepare(`
    INSERT INTO complaint_trends (model_id, cal_year, complaint_count, crash_count, fire_count, injury_count, death_count)
    SELECT
      my.model_id,
      CAST(SUBSTR(c.date_added, 1, 4) AS INTEGER) as cal_year,
      COUNT(*) as complaint_count,
      SUM(CASE WHEN c.crash = 'Y' THEN 1 ELSE 0 END) as crash_count,
      SUM(CASE WHEN c.fire = 'Y' THEN 1 ELSE 0 END) as fire_count,
      SUM(c.injured) as injury_count,
      SUM(c.deaths) as death_count
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    WHERE c.date_added IS NOT NULL AND LENGTH(c.date_added) >= 4
      AND CAST(SUBSTR(c.date_added, 1, 4) AS INTEGER) BETWEEN 1995 AND 2026
    GROUP BY my.model_id, cal_year
  `).run();
  const trendCount = db.prepare('SELECT COUNT(*) as cnt FROM complaint_trends').get().cnt;
  console.log('    ' + trendCount + ' trend rows');

  // Component summary: aggregate stats per component across all vehicles
  console.log('  Building component_summary...');
  db.prepare('DELETE FROM component_summary').run();
  db.prepare(`
    INSERT INTO component_summary (component_slug, component, complaint_count, crash_count, fire_count, injury_count, death_count, affected_makes, affected_models, top_models)
    SELECT
      LOWER(REPLACE(REPLACE(REPLACE(REPLACE(component, ' ', '-'), ':', '-'), '/', '-'), '--', '-')) as component_slug,
      component,
      COUNT(*) as complaint_count,
      SUM(CASE WHEN crash = 'Y' THEN 1 ELSE 0 END) as crash_count,
      SUM(CASE WHEN fire = 'Y' THEN 1 ELSE 0 END) as fire_count,
      SUM(injured) as injury_count,
      SUM(deaths) as death_count,
      COUNT(DISTINCT my.make_id) as affected_makes,
      COUNT(DISTINCT my.model_id) as affected_models,
      NULL as top_models
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    WHERE component IS NOT NULL AND component != ''
    GROUP BY component
    HAVING complaint_count >= 5
  `).run();
  const compCount = db.prepare('SELECT COUNT(*) as cnt FROM component_summary').get().cnt;
  console.log('    ' + compCount + ' component summary rows');

  // Year summary: aggregate stats per vehicle model year
  console.log('  Building year_summary...');
  db.prepare('DELETE FROM year_summary').run();
  db.prepare(`
    INSERT INTO year_summary (year, complaint_count, crash_count, fire_count, injury_count, death_count, make_count, model_count, top_components)
    SELECT
      my.year,
      SUM(my.complaint_count) as complaint_count,
      SUM(my.crash_count) as crash_count,
      SUM(my.fire_count) as fire_count,
      SUM(my.injury_count) as injury_count,
      SUM(my.death_count) as death_count,
      COUNT(DISTINCT my.make_id) as make_count,
      COUNT(DISTINCT my.model_id) as model_count,
      NULL as top_components
    FROM model_years my
    GROUP BY my.year
    ORDER BY my.year DESC
  `).run();
  const yearCount = db.prepare('SELECT COUNT(*) as cnt FROM year_summary').get().cnt;
  console.log('    ' + yearCount + ' year summary rows');

  // --- Composite Indexes ---
  console.log('\nBuilding composite indexes...');
  db.prepare('CREATE INDEX IF NOT EXISTS idx_model_years_make_year ON model_years(make_id, year DESC)').run();
  db.prepare('CREATE INDEX IF NOT EXISTS idx_complaints_my_date ON complaints(my_id, date_added DESC)').run();

  // --- Finalization ---
  console.log('Analyzing and finalizing...');
  db.pragma('journal_mode = DELETE');
  db.prepare('ANALYZE').run();
  db.prepare('VACUUM').run();

  db.close();
  console.log('Done! Database at ' + DB_PATH);
}

main().catch(err => { console.error(err); process.exit(1); });
