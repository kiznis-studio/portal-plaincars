#!/usr/bin/env node
// Parse NHTSA complaints flat file into local SQLite DB
// Input: /storage/plaincars/FLAT_CMPL.txt (pipe-delimited, 2.18M rows)
// Output: data/plaincars.db

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

    // Store individual complaints for 2018+ or notable
    if (yearRaw >= 2018 || deaths > 0 || injured > 0 || crash === 'Y' || fire === 'Y') {
      complaints.push({
        cmplid, odiNumber, myKey,
        crash, fire, injured, deaths,
        component, summary: summary.slice(0, 500),
        fail_date: (fields[7] || '').trim(),
        date_added: dateAdded,
        mileage, state,
      });
    }
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
    'INSERT OR REPLACE INTO complaints (cmplid, my_id, odi_number, crash, fire, injured, deaths, component, summary, fail_date, date_added, mileage, state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)'
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
        insertComplaint.run(c.cmplid, myId, c.odiNumber, c.crash, c.fire, c.injured, c.deaths, c.component, c.summary, c.fail_date, c.date_added, c.mileage, c.state);
      }
    })();
    complaintInserted += batch.length;
    if (complaintInserted % 200000 === 0) console.log('  Complaints: ' + complaintInserted + '...');
  }
  console.log('  Inserted ' + complaintInserted + ' individual complaints');

  db.close();
  console.log('Done! Database at ' + DB_PATH);
}

main().catch(err => { console.error(err); process.exit(1); });
