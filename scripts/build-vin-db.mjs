#!/usr/bin/env node
/**
 * build-vin-db.mjs — Build local VIN decoder SQLite DB from NHTSA vPIC PostgreSQL dump
 *
 * Downloads and parses the vPIC flat file to create a compact SQLite DB for
 * offline VIN decoding. Eliminates runtime NHTSA API calls for ~95% of VINs.
 *
 * Usage: node scripts/build-vin-db.mjs /path/to/vPICList_lite_*.sql [output.db]
 *
 * The vPIC dump is a PostgreSQL plain-text backup. We parse the COPY blocks
 * for the tables we need and insert into SQLite.
 *
 * Key tables extracted:
 *   - wmi: World Manufacturer Identifier (VIN chars 1-3) → manufacturer/make
 *   - make, model: Vehicle make and model names
 *   - pattern: VIN pattern matching rules (chars 4-8) → vehicle attributes
 *   - element: Attribute definitions (what each pattern field means)
 *   - vinschema, wmi_vinschema: Links WMIs to pattern schemas
 *   - Lookup tables: BodyStyle, DriveType, FuelType, VehicleType, Country, etc.
 */

import { createReadStream, statSync } from 'fs';
import { createInterface } from 'readline';
import Database from 'better-sqlite3';

const VPIC_SQL = process.argv[2];
const OUTPUT_DB = process.argv[3] || 'data/vin-decoder.db';

if (!VPIC_SQL) {
  console.error('Usage: node scripts/build-vin-db.mjs <vPIC-sql-file> [output.db]');
  process.exit(1);
}

// Element IDs we care about for VIN decoding
const ELEMENT_MAP = {
  5: 'body_class',        // Body Class
  9: 'engine_cylinders',  // Engine Number of Cylinders
  11: 'displacement_cc',  // Displacement (CC)
  13: 'displacement_l',   // Displacement (L)
  15: 'drive_type',       // Drive Type
  24: 'fuel_type',        // Fuel Type - Primary
  25: 'gvwr',             // Gross Vehicle Weight Rating From
  31: 'plant_city',       // Plant City
  39: 'vehicle_type',     // Vehicle Type
  64: 'engine_config',    // Engine Configuration
  75: 'plant_country',    // Plant Country
  77: 'plant_state',      // Plant State
  126: 'electrification', // Electrification Level
};

// Lookup tables we need to resolve attribute IDs to names
const LOOKUP_TABLES = [
  'bodystyle', 'drivetype', 'fueltype', 'vehicletype', 'country',
  'grossvehicleweightrating', 'engineconfiguration', 'electrificationlevel',
];

console.log('Building VIN decoder DB from vPIC dump...');
console.log(`Input:  ${VPIC_SQL}`);
console.log(`Output: ${OUTPUT_DB}`);

// Create SQLite DB
const db = new Database(OUTPUT_DB);
db.pragma('journal_mode = WAL');
db.pragma('synchronous = OFF');

// Create schema
db.exec(`
  -- WMI: World Manufacturer Identifier (VIN chars 1-3 or 1-6 for small manufacturers)
  CREATE TABLE IF NOT EXISTS wmi (
    id INTEGER PRIMARY KEY,
    wmi TEXT NOT NULL,
    manufacturer_id INTEGER,
    make_id INTEGER,
    vehicle_type_id INTEGER,
    country_id INTEGER
  );

  CREATE TABLE IF NOT EXISTS wmi_make (
    wmi_id INTEGER NOT NULL,
    make_id INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS wmi_vinschema (
    id INTEGER PRIMARY KEY,
    wmi_id INTEGER NOT NULL,
    vinschema_id INTEGER NOT NULL,
    year_from INTEGER NOT NULL,
    year_to INTEGER
  );

  -- Makes and Models
  CREATE TABLE IF NOT EXISTS make (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS model (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS make_model (
    id INTEGER PRIMARY KEY,
    make_id INTEGER NOT NULL,
    model_id INTEGER NOT NULL
  );

  CREATE TABLE IF NOT EXISTS manufacturer (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL
  );

  -- VIN Schemas and Patterns
  CREATE TABLE IF NOT EXISTS vinschema (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    source_wmi TEXT
  );

  -- Patterns: VIN pattern matching rules
  -- keys = wildcard pattern for VIN positions (e.g., "???A?????" matches char 4 = A)
  -- element_id = what vehicle attribute this pattern defines
  -- attribute_id = the value (may be a lookup table ID or literal string)
  CREATE TABLE IF NOT EXISTS pattern (
    id INTEGER PRIMARY KEY,
    vinschema_id INTEGER NOT NULL,
    keys TEXT NOT NULL,
    element_id INTEGER NOT NULL,
    attribute_id TEXT NOT NULL
  );

  -- Lookup tables for resolving attribute IDs to names
  CREATE TABLE IF NOT EXISTS lookup (
    table_name TEXT NOT NULL,
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (table_name, id)
  );

  -- Element definitions
  CREATE TABLE IF NOT EXISTS element (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    code TEXT,
    lookup_table TEXT,
    data_type TEXT
  );

  -- Year code mapping
  CREATE TABLE IF NOT EXISTS year_code (
    code TEXT NOT NULL,
    year INTEGER NOT NULL,
    cycle INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (code, cycle)
  );
`);

// Parse PostgreSQL COPY blocks
async function parseCopyBlocks(sqlFile) {
  const tables = {};
  let currentTable = null;
  let currentColumns = null;

  const rl = createInterface({
    input: createReadStream(sqlFile, { encoding: 'utf-8' }),
    crlfDelay: Infinity,
  });

  let lineCount = 0;
  for await (const line of rl) {
    lineCount++;
    if (lineCount % 1000000 === 0) console.log(`  Parsed ${(lineCount / 1000000).toFixed(1)}M lines...`);

    if (line.startsWith('COPY vpic.')) {
      // Parse: COPY vpic.tablename (col1, col2, ...) FROM stdin;
      const match = line.match(/^COPY vpic\.(\S+)\s+\(([^)]+)\)/);
      if (match) {
        const tableName = match[1].toLowerCase();
        const columns = match[2].split(',').map(c => c.trim().replace(/"/g, ''));
        currentTable = tableName;
        currentColumns = columns;
        if (!tables[tableName]) tables[tableName] = { columns, rows: [] };
      }
      continue;
    }

    if (line === '\\.') {
      currentTable = null;
      currentColumns = null;
      continue;
    }

    if (currentTable && currentColumns) {
      const values = line.split('\t');
      const row = {};
      for (let i = 0; i < currentColumns.length && i < values.length; i++) {
        row[currentColumns[i]] = values[i] === '\\N' ? null : values[i];
      }
      tables[currentTable].rows.push(row);
    }
  }

  return tables;
}

console.log('Parsing vPIC SQL dump (this may take a minute)...');
const tables = await parseCopyBlocks(VPIC_SQL);

console.log('Tables found:');
for (const [name, data] of Object.entries(tables)) {
  console.log(`  ${name}: ${data.rows.length} rows`);
}

// Insert data into SQLite
db.exec('BEGIN');

// WMI
if (tables.wmi) {
  const stmt = db.prepare('INSERT OR REPLACE INTO wmi (id, wmi, manufacturer_id, make_id, vehicle_type_id, country_id) VALUES (?, ?, ?, ?, ?, ?)');
  for (const r of tables.wmi.rows) {
    stmt.run(+r.id, r.wmi, r.manufacturerid ? +r.manufacturerid : null, r.makeid ? +r.makeid : null, r.vehicletypeid ? +r.vehicletypeid : null, r.countryid ? +r.countryid : null);
  }
  console.log(`Inserted ${tables.wmi.rows.length} WMI records`);
}

// WMI-Make mapping
if (tables.wmi_make) {
  const stmt = db.prepare('INSERT INTO wmi_make (wmi_id, make_id) VALUES (?, ?)');
  for (const r of tables.wmi_make.rows) {
    stmt.run(+r.wmiid, +r.makeid);
  }
  console.log(`Inserted ${tables.wmi_make.rows.length} WMI-Make mappings`);
}

// WMI-VINSchema mapping
if (tables.wmi_vinschema) {
  const stmt = db.prepare('INSERT OR REPLACE INTO wmi_vinschema (id, wmi_id, vinschema_id, year_from, year_to) VALUES (?, ?, ?, ?, ?)');
  for (const r of tables.wmi_vinschema.rows) {
    stmt.run(+r.id, +r.wmiid, +r.vinschemaid, +r.yearfrom, r.yearto ? +r.yearto : null);
  }
  console.log(`Inserted ${tables.wmi_vinschema.rows.length} WMI-VINSchema mappings`);
}

// Makes
if (tables.make) {
  const stmt = db.prepare('INSERT OR REPLACE INTO make (id, name) VALUES (?, ?)');
  for (const r of tables.make.rows) {
    stmt.run(+r.id, r.name);
  }
  console.log(`Inserted ${tables.make.rows.length} makes`);
}

// Models
if (tables.model) {
  const stmt = db.prepare('INSERT OR REPLACE INTO model (id, name) VALUES (?, ?)');
  for (const r of tables.model.rows) {
    stmt.run(+r.id, r.name);
  }
  console.log(`Inserted ${tables.model.rows.length} models`);
}

// Make-Model mapping
if (tables.make_model) {
  const stmt = db.prepare('INSERT OR REPLACE INTO make_model (id, make_id, model_id) VALUES (?, ?, ?)');
  for (const r of tables.make_model.rows) {
    stmt.run(+r.id, +r.makeid, +r.modelid);
  }
  console.log(`Inserted ${tables.make_model.rows.length} make-model mappings`);
}

// Manufacturers
if (tables.manufacturer) {
  const stmt = db.prepare('INSERT OR REPLACE INTO manufacturer (id, name) VALUES (?, ?)');
  for (const r of tables.manufacturer.rows) {
    stmt.run(+r.id, r.name);
  }
  console.log(`Inserted ${tables.manufacturer.rows.length} manufacturers`);
}

// VIN Schemas
if (tables.vinschema) {
  const stmt = db.prepare('INSERT OR REPLACE INTO vinschema (id, name, source_wmi) VALUES (?, ?, ?)');
  for (const r of tables.vinschema.rows) {
    stmt.run(+r.id, r.name, r.sourcewmi);
  }
  console.log(`Inserted ${tables.vinschema.rows.length} VIN schemas`);
}

// Elements
if (tables.element) {
  const stmt = db.prepare('INSERT OR REPLACE INTO element (id, name, code, lookup_table, data_type) VALUES (?, ?, ?, ?, ?)');
  for (const r of tables.element.rows) {
    stmt.run(+r.id, r.name, r.code, r.lookuptable, r.datatype);
  }
  console.log(`Inserted ${tables.element.rows.length} elements`);
}

// Patterns — only insert patterns for elements we care about (saves ~80% space)
if (tables.pattern) {
  const relevantElements = new Set(Object.keys(ELEMENT_MAP).map(Number));
  // Also include model-related elements (26=Make, 28=Model, 34=Series, 29=Trim)
  [26, 28, 34, 29].forEach(id => relevantElements.add(id));

  const stmt = db.prepare('INSERT INTO pattern (id, vinschema_id, keys, element_id, attribute_id) VALUES (?, ?, ?, ?, ?)');
  let inserted = 0;
  for (const r of tables.pattern.rows) {
    const elementId = +r.elementid;
    if (relevantElements.has(elementId)) {
      stmt.run(+r.id, +r.vinschemaid, r.keys, elementId, r.attributeid);
      inserted++;
    }
  }
  console.log(`Inserted ${inserted} patterns (filtered from ${tables.pattern.rows.length} total)`);
}

// Lookup tables
for (const lookupName of LOOKUP_TABLES) {
  if (tables[lookupName]) {
    const stmt = db.prepare('INSERT OR REPLACE INTO lookup (table_name, id, name) VALUES (?, ?, ?)');
    for (const r of tables[lookupName].rows) {
      if (r.id && r.name) {
        stmt.run(lookupName, +r.id, r.name);
      }
    }
    console.log(`Inserted ${tables[lookupName].rows.length} ${lookupName} lookups`);
  }
}

// Year codes
const yearCodeStmt = db.prepare('INSERT OR REPLACE INTO year_code (code, year, cycle) VALUES (?, ?, ?)');
const chars = 'ABCDEFGHJKLMNPRSTVWXY123456789';
for (let i = 0; i < chars.length; i++) {
  yearCodeStmt.run(chars[i], 1980 + i, 1);
  yearCodeStmt.run(chars[i], 2010 + i, 2);
}
console.log('Inserted year code mappings');

db.exec('COMMIT');

// Create indexes
console.log('Creating indexes...');
db.exec(`
  CREATE INDEX IF NOT EXISTS idx_wmi_code ON wmi(wmi);
  CREATE INDEX IF NOT EXISTS idx_wmi_make_wmiid ON wmi_make(wmi_id);
  CREATE INDEX IF NOT EXISTS idx_wmi_vinschema_wmiid ON wmi_vinschema(wmi_id);
  CREATE INDEX IF NOT EXISTS idx_pattern_schema_element ON pattern(vinschema_id, element_id);
  CREATE INDEX IF NOT EXISTS idx_make_model_makeid ON make_model(make_id);
  CREATE INDEX IF NOT EXISTS idx_make_model_modelid ON make_model(model_id);
`);

// ANALYZE and compact
console.log('Running ANALYZE...');
db.exec('ANALYZE');
db.pragma('journal_mode = DELETE');
db.exec('VACUUM');

// Report
const wmiCount = db.prepare('SELECT COUNT(*) as n FROM wmi').get().n;
const patternCount = db.prepare('SELECT COUNT(*) as n FROM pattern').get().n;
const makeCount = db.prepare('SELECT COUNT(*) as n FROM make').get().n;
const modelCount = db.prepare('SELECT COUNT(*) as n FROM model').get().n;

console.log(`\nVIN Decoder DB built successfully!`);
console.log(`  WMIs: ${wmiCount}`);
console.log(`  Makes: ${makeCount}`);
console.log(`  Models: ${modelCount}`);
console.log(`  Patterns: ${patternCount}`);

db.close();

const size = statSync(OUTPUT_DB).size;
console.log(`  DB size: ${(size / 1024 / 1024).toFixed(1)} MB`);
console.log('\nDone!');
