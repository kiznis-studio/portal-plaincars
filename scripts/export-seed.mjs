#!/usr/bin/env node
// Export local SQLite → chunked SQL seed files for D1

import Database from 'better-sqlite3';
import { writeFileSync, readFileSync, mkdirSync, existsSync } from 'fs';
import { resolve, dirname } from 'path';
import { fileURLToPath } from 'url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DB_PATH = resolve(__dirname, '../data/plaincars.db');
const SEED_DIR = resolve(__dirname, '../data/seed');
const CHUNK_SIZE = 500;

function esc(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number') return String(v);
  return "'" + String(v).replace(/'/g, "''") + "'";
}

function exportTable(db, table, columns, filename, orderBy = '') {
  const order = orderBy ? ` ORDER BY ${orderBy}` : '';
  const rows = db.prepare(`SELECT ${columns.join(',')} FROM ${table}${order}`).all();

  let fileIdx = 0;
  for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
    const chunk = rows.slice(i, i + CHUNK_SIZE);
    const stmts = chunk.map(row => {
      const vals = columns.map(c => esc(row[c]));
      return `INSERT INTO ${table} (${columns.join(',')}) VALUES (${vals.join(',')});`;
    });

    const suffix = rows.length > CHUNK_SIZE ? `-${String(fileIdx).padStart(3, '0')}` : '';
    writeFileSync(resolve(SEED_DIR, `${filename}${suffix}.sql`), stmts.join('\n') + '\n');
    fileIdx++;
  }

  console.log(`  ${table}: ${rows.length} rows → ${fileIdx} file(s)`);
}

function main() {
  const db = new Database(DB_PATH, { readonly: true });

  if (!existsSync(SEED_DIR)) mkdirSync(SEED_DIR, { recursive: true });

  // Write schema first
  const schema = readFileSync(resolve(__dirname, '../data/schema.sql'), 'utf8');
  writeFileSync(resolve(SEED_DIR, '000-schema.sql'), schema);
  console.log('Exported schema');

  console.log('Exporting tables...');
  exportTable(db, 'makes', ['make_id','make_name','slug','complaint_count','recall_count','model_count'], '001-makes');
  exportTable(db, 'models', ['model_id','make_id','model_name','slug','year_min','year_max','complaint_count','recall_count'], '002-models');
  exportTable(db, 'model_years', ['my_id','model_id','make_id','year','complaint_count','crash_count','fire_count','injury_count','death_count','recall_count','overall_rating','front_crash_rating','side_crash_rating','rollover_rating','rollover_risk'], '003-model_years', 'make_id, year');
  exportTable(db, 'complaint_stats', ['my_id','component','complaint_count','crash_count','fire_count','injury_count','death_count','sample_text'], '004-complaint_stats', 'my_id');
  exportTable(db, 'recalls', ['recall_id','campaign_number','my_id','make_id','model_id','year','component','summary','consequence','remedy','report_date','affected_count'], '005-recalls', 'report_date DESC');

  // Individual complaints — large table, only export 2018+
  const complaintCols = ['cmplid','my_id','odi_number','crash','fire','injured','deaths','component','summary','fail_date','date_added','mileage','state'];
  exportTable(db, 'complaints', complaintCols, '006-complaints', 'date_added DESC');

  // Investigations
  exportTable(db, 'investigations', ['nhtsa_id','subject','investigation_type','status','open_date','latest_activity_date','description','make_id','model_id'], '007-investigations', 'open_date DESC');

  db.close();
  console.log('Done! Seed files at:', SEED_DIR);
}

main();
