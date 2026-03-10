/**
 * Local VIN Decoder — uses NHTSA vPIC SQLite DB for offline VIN decoding.
 *
 * Decodes VIN → make, model, year, body class, drive type, engine specs, plant info.
 *
 * Priority chain:
 *   1. WMI table → Make & Manufacturer (always works, chars 1-3)
 *   2. Year code → Model Year (always works, char 10)
 *   3. Pattern matching → Body, Drive, Engine, Plant (when pattern matches)
 *   4. API cache → saved results from previous NHTSA API fallbacks
 *   5. NHTSA API fallback → full decode (saves to API cache for next time)
 *
 * The vin-decoder.db is mounted :ro alongside plaincars.db in the Docker container.
 * The API cache lives in a writable volume at /data/cache/vin-api-cache.db.
 *
 * Cache key: VIN descriptor (chars 1-8) — all VINs sharing WMI+VDS decode
 * to the same make/model/specs. Year is stored separately (char 10 = year code).
 * One cached API response covers thousands of serial number variants.
 */

export interface VinDecodeResult {
  vin: string;
  make: string;
  model: string;
  year: string;
  bodyClass: string;
  driveType: string;
  engineCylinders: string;
  displacementL: string;
  fuelType: string;
  plantCity: string;
  plantState: string;
  plantCountry: string;
  vehicleType: string;
  gvwr: string;
  manufacturer: string;
  errorCode: string;
  errorText: string;
  source: 'local' | 'api' | 'partial';
}

// Year code mapping: VIN position 10 → model year
// Cycles: 1980-2009 (cycle 1), 2010-2039 (cycle 2)
const YEAR_CODES: Record<string, number[]> = {};
const yearChars = 'ABCDEFGHJKLMNPRSTVWXY123456789';
for (let i = 0; i < yearChars.length; i++) {
  YEAR_CODES[yearChars[i]] = [1980 + i, 2010 + i];
}

// Disambiguate year code using VIN position 7 (model year qualifier)
// Per 49 CFR 565.15: position 7 is numeric for cycle 1 (1980-2009),
// alphabetic for cycle 2 (2010-2039)
function resolveYear(yearCode: string, pos7: string): number {
  const years = YEAR_CODES[yearCode.toUpperCase()];
  if (!years) return 0;
  if (years.length === 1) return years[0];
  // If position 7 is alphabetic, it's cycle 2 (2010+)
  // If position 7 is numeric, it's cycle 1 (1980-2009)
  return /[A-Z]/i.test(pos7) ? years[1] : years[0];
}

// Element IDs for the fields we decode
const ELEMENT_IDS = {
  BODY_CLASS: 5,
  ENGINE_CYLINDERS: 9,
  DISPLACEMENT_L: 13,
  DRIVE_TYPE: 15,
  FUEL_TYPE: 24,
  GVWR: 25,
  PLANT_CITY: 31,
  VEHICLE_TYPE: 39,
  ENGINE_CONFIG: 64,
  PLANT_COUNTRY: 75,
  PLANT_STATE: 77,
  MODEL: 28,
  MAKE: 26,
};

// Lookup table names for resolving attribute IDs
const LOOKUP_MAP: Record<number, string> = {
  [ELEMENT_IDS.BODY_CLASS]: 'bodystyle',
  [ELEMENT_IDS.DRIVE_TYPE]: 'drivetype',
  [ELEMENT_IDS.FUEL_TYPE]: 'fueltype',
  [ELEMENT_IDS.VEHICLE_TYPE]: 'vehicletype',
  [ELEMENT_IDS.GVWR]: 'grossvehicleweightrating',
  [ELEMENT_IDS.ENGINE_CONFIG]: 'engineconfiguration',
  [ELEMENT_IDS.PLANT_COUNTRY]: 'country',
};

/**
 * Match a VIN's VDS section against a vPIC pattern key.
 *
 * Pattern format examples:
 *   "CB7[12]"  → pos4=C, pos5=B, pos6=7, pos7=1or2
 *   "*****|*N" → wildcard VDS, plant pos=N
 *   "A[BCD]"   → pos4=A, pos5=B,C,or D
 *   "?"        → single char wildcard
 *   "*"        → multi-char wildcard
 */
function matchPattern(vin: string, pattern: string): boolean {
  // Pattern can have pipe for multi-part matching: "vds_pattern|plant_pattern"
  const parts = pattern.split('|');

  for (const part of parts) {
    // Build a regex from the vPIC pattern
    let regex = '^';
    let i = 0;
    while (i < part.length) {
      const ch = part[i];
      if (ch === '*') {
        regex += '.*';
      } else if (ch === '?') {
        regex += '.';
      } else if (ch === '[') {
        const end = part.indexOf(']', i);
        if (end >= 0) {
          regex += part.slice(i, end + 1);
          i = end;
        } else {
          regex += '\\' + ch;
        }
      } else {
        regex += ch.replace(/[.*+?^${}()|\\]/g, '\\$&');
      }
      i++;
    }
    regex += '$';

    // For single-part patterns, match against VDS (chars 4-8, 0-indexed 3-7)
    // For multi-part with |, first part = VDS, second part = plant area
    try {
      const re = new RegExp(regex, 'i');
      if (parts.length === 1) {
        // Match against VDS (positions 4-8, 0-indexed)
        const vds = vin.slice(3, 8);
        if (!re.test(vds)) return false;
      } else {
        // First part matches VDS, subsequent parts match remaining positions
        const partIndex = parts.indexOf(part);
        if (partIndex === 0) {
          const vds = vin.slice(3, 8);
          if (!re.test(vds)) return false;
        } else {
          // Match against plant code area (positions 9-11, 0-indexed)
          const plantArea = vin.slice(8, 11);
          if (!re.test(plantArea)) return false;
        }
      }
    } catch {
      return false;
    }
  }
  return true;
}

/**
 * Decode a VIN using the local vPIC database.
 */
export async function decodeVinLocal(
  vinDb: any, // D1Database for vin-decoder.db
  vin: string
): Promise<Partial<VinDecodeResult>> {
  const v = vin.toUpperCase();
  if (v.length !== 17) return { vin: v, errorCode: '5', errorText: 'VIN must be 17 characters' };

  const result: Partial<VinDecodeResult> = { vin: v, source: 'local' };

  // Step 1: Decode year from position 10
  const yearCode = v[9];
  const pos7 = v[6];
  const year = resolveYear(yearCode, pos7);
  if (year) result.year = String(year);

  // Step 2: Look up WMI (chars 1-3)
  const wmi3 = v.slice(0, 3);
  const wmiRow = await vinDb.prepare(`
    SELECT w.id, w.manufacturer_id, w.vehicle_type_id, m.name as make_name, mfr.name as mfr_name
    FROM wmi w
    LEFT JOIN wmi_make wm ON wm.wmi_id = w.id
    LEFT JOIN make m ON m.id = wm.make_id
    LEFT JOIN manufacturer mfr ON mfr.id = w.manufacturer_id
    WHERE w.wmi = ?
    LIMIT 1
  `).bind(wmi3).first<{ id: number; manufacturer_id: number; vehicle_type_id: number; make_name: string; mfr_name: string }>();

  if (wmiRow) {
    if (wmiRow.make_name) result.make = wmiRow.make_name;
    if (wmiRow.mfr_name) result.manufacturer = wmiRow.mfr_name;

    // Resolve vehicle type from lookup
    if (wmiRow.vehicle_type_id) {
      const vt = await vinDb.prepare('SELECT name FROM lookup WHERE table_name = ? AND id = ?')
        .bind('vehicletype', wmiRow.vehicle_type_id).first<{ name: string }>();
      if (vt) result.vehicleType = vt.name;
    }

    // Step 3: Find matching VIN schema for this WMI + year
    if (year) {
      const schemas = await vinDb.prepare(`
        SELECT wvs.vinschema_id
        FROM wmi_vinschema wvs
        WHERE wvs.wmi_id = ? AND wvs.year_from <= ? AND (wvs.year_to >= ? OR wvs.year_to IS NULL)
      `).bind(wmiRow.id, year, year).all<{ vinschema_id: number }>();

      if (schemas.results.length > 0) {
        const schemaIds = schemas.results.map(s => s.vinschema_id);

        // Step 4: Match patterns against VIN
        for (const schemaId of schemaIds) {
          const patterns = await vinDb.prepare(`
            SELECT p.keys, p.element_id, p.attribute_id, e.name as element_name, e.lookup_table, e.data_type
            FROM pattern p
            JOIN element e ON e.id = p.element_id
            WHERE p.vinschema_id = ?
          `).bind(schemaId).all<{
            keys: string; element_id: number; attribute_id: string;
            element_name: string; lookup_table: string; data_type: string;
          }>();

          for (const p of patterns.results) {
            if (!matchPattern(v, p.keys)) continue;

            // Resolve the attribute value
            let value = p.attribute_id;
            const lookupTable = LOOKUP_MAP[p.element_id];
            if (lookupTable && p.data_type === 'lookup') {
              const lookup = await vinDb.prepare('SELECT name FROM lookup WHERE table_name = ? AND id = ?')
                .bind(lookupTable, parseInt(value)).first<{ name: string }>();
              if (lookup) value = lookup.name;
            } else if (p.element_id === ELEMENT_IDS.MODEL) {
              const model = await vinDb.prepare('SELECT name FROM model WHERE id = ?')
                .bind(parseInt(value)).first<{ name: string }>();
              if (model) value = model.name;
            } else if (p.element_id === ELEMENT_IDS.MAKE) {
              const make = await vinDb.prepare('SELECT name FROM make WHERE id = ?')
                .bind(parseInt(value)).first<{ name: string }>();
              if (make) value = make.name;
            }

            // Map element to result field
            switch (p.element_id) {
              case ELEMENT_IDS.BODY_CLASS: result.bodyClass = value; break;
              case ELEMENT_IDS.ENGINE_CYLINDERS: result.engineCylinders = value; break;
              case ELEMENT_IDS.DISPLACEMENT_L: result.displacementL = value; break;
              case ELEMENT_IDS.DRIVE_TYPE: result.driveType = value; break;
              case ELEMENT_IDS.FUEL_TYPE: result.fuelType = value; break;
              case ELEMENT_IDS.GVWR: result.gvwr = value; break;
              case ELEMENT_IDS.PLANT_CITY: result.plantCity = value; break;
              case ELEMENT_IDS.PLANT_STATE: result.plantState = value; break;
              case ELEMENT_IDS.PLANT_COUNTRY: result.plantCountry = value; break;
              case ELEMENT_IDS.VEHICLE_TYPE: result.vehicleType = value; break;
              case ELEMENT_IDS.MODEL: if (!result.model) result.model = value; break;
              case ELEMENT_IDS.MAKE: if (!result.make) result.make = value; break;
            }
          }
        }
      }
    }
  }

  // Step 5: If vPIC pattern matching didn't get make+model, check api_patterns table
  // (populated by merge-vin-cache.mjs from previous API fallback results)
  if ((!result.make || !result.model) && result.year) {
    try {
      const descriptor = v.slice(0, 8);
      const apiRow = await vinDb.prepare(
        'SELECT * FROM api_patterns WHERE descriptor = ? AND year = ?'
      ).bind(descriptor, result.year).first<any>();
      if (apiRow && apiRow.make && apiRow.model) {
        result.make = result.make || apiRow.make;
        result.model = result.model || apiRow.model;
        if (!result.bodyClass && apiRow.body_class) result.bodyClass = apiRow.body_class;
        if (!result.driveType && apiRow.drive_type) result.driveType = apiRow.drive_type;
        if (!result.engineCylinders && apiRow.engine_cylinders) result.engineCylinders = apiRow.engine_cylinders;
        if (!result.displacementL && apiRow.displacement_l) result.displacementL = apiRow.displacement_l;
        if (!result.fuelType && apiRow.fuel_type) result.fuelType = apiRow.fuel_type;
        if (!result.plantCity && apiRow.plant_city) result.plantCity = apiRow.plant_city;
        if (!result.plantState && apiRow.plant_state) result.plantState = apiRow.plant_state;
        if (!result.plantCountry && apiRow.plant_country) result.plantCountry = apiRow.plant_country;
        if (!result.vehicleType && apiRow.vehicle_type) result.vehicleType = apiRow.vehicle_type;
        if (!result.gvwr && apiRow.gvwr) result.gvwr = apiRow.gvwr;
        if (!result.manufacturer && apiRow.manufacturer) result.manufacturer = apiRow.manufacturer;
      }
    } catch {
      // api_patterns table may not exist yet — that's fine
    }
  }

  // Determine decode quality
  if (result.make && result.model && result.year) {
    result.errorCode = '0';
    result.source = 'local';
  } else if (result.make || result.year) {
    result.errorCode = '1'; // Partial decode
    result.source = 'partial';
  } else {
    result.errorCode = '6'; // Not found
    result.errorText = 'VIN not found in local database';
  }

  return result;
}

// --- API Cache: writable SQLite that learns from NHTSA API fallbacks ---

import Database from 'better-sqlite3';
import { existsSync, mkdirSync } from 'node:fs';
import { dirname } from 'node:path';

// VIN descriptor = chars 1-8 (WMI + VDS). All VINs with the same descriptor
// decode to the same make/model/specs. Year comes from char 10 separately.
function vinDescriptor(vin: string): string {
  return vin.slice(0, 8);
}

let apiCacheDb: InstanceType<typeof Database> | null = null;
let apiCacheReady = false;
let apiCacheStats = { hits: 0, misses: 0, writes: 0 };

export function getApiCacheStats() { return apiCacheStats; }

function getApiCache(cachePath: string | undefined): InstanceType<typeof Database> | null {
  if (apiCacheDb) return apiCacheDb;
  if (apiCacheReady) return null; // Already tried, failed
  apiCacheReady = true;

  if (!cachePath) return null;

  try {
    const dir = dirname(cachePath);
    if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
    apiCacheDb = new Database(cachePath);
    apiCacheDb.pragma('journal_mode = WAL');
    apiCacheDb.pragma('synchronous = NORMAL');
    apiCacheDb.exec(`
      CREATE TABLE IF NOT EXISTS vin_cache (
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
        cached_at TEXT NOT NULL DEFAULT (datetime('now')),
        hit_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (descriptor, year)
      );
    `);
    return apiCacheDb;
  } catch (err) {
    console.warn('[vin-cache] Failed to open API cache:', err);
    return null;
  }
}

function lookupApiCache(
  cachePath: string | undefined, descriptor: string, year: string
): VinDecodeResult | null {
  const db = getApiCache(cachePath);
  if (!db) return null;

  try {
    const row = db.prepare(
      'SELECT * FROM vin_cache WHERE descriptor = ? AND year = ?'
    ).get(descriptor, year) as any;
    if (!row || !row.make || !row.model) return null;

    // Bump hit count (fire-and-forget)
    try {
      db.prepare('UPDATE vin_cache SET hit_count = hit_count + 1 WHERE descriptor = ? AND year = ?')
        .run(descriptor, year);
    } catch { /* non-critical */ }

    apiCacheStats.hits++;
    return {
      vin: '', // Caller fills this in
      make: row.make,
      model: row.model,
      year: row.year,
      bodyClass: row.body_class,
      driveType: row.drive_type,
      engineCylinders: row.engine_cylinders,
      displacementL: row.displacement_l,
      fuelType: row.fuel_type,
      plantCity: row.plant_city,
      plantState: row.plant_state,
      plantCountry: row.plant_country,
      vehicleType: row.vehicle_type,
      gvwr: row.gvwr,
      manufacturer: row.manufacturer,
      errorCode: '0',
      errorText: '',
      source: 'api', // Originally from API
    };
  } catch {
    return null;
  }
}

function saveToApiCache(
  cachePath: string | undefined, descriptor: string, decoded: VinDecodeResult
): void {
  const db = getApiCache(cachePath);
  if (!db || !decoded.make || !decoded.model) return;

  try {
    db.prepare(`
      INSERT OR REPLACE INTO vin_cache
        (descriptor, year, make, model, body_class, drive_type, engine_cylinders,
         displacement_l, fuel_type, plant_city, plant_state, plant_country,
         vehicle_type, gvwr, manufacturer)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      descriptor, decoded.year, decoded.make, decoded.model,
      decoded.bodyClass, decoded.driveType, decoded.engineCylinders,
      decoded.displacementL, decoded.fuelType, decoded.plantCity,
      decoded.plantState, decoded.plantCountry, decoded.vehicleType,
      decoded.gvwr, decoded.manufacturer
    );
    apiCacheStats.writes++;
  } catch (err) {
    console.warn('[vin-cache] Failed to save:', err);
  }
}

/**
 * Full VIN decode with local-first, cache, API-fallback strategy.
 *
 * Priority: local vPIC DB → API cache → NHTSA API (saved to cache).
 * Cache key is VIN descriptor (chars 1-8) + year, so one API call
 * covers all serial number variants of the same vehicle.
 */
export async function decodeVin(
  vinDb: any | null,
  vin: string,
  apiCachePath?: string
): Promise<{ decoded: VinDecodeResult; source: 'local' | 'api' | 'api-cached' | 'partial' }> {
  const v = vin.toUpperCase();
  const desc = vinDescriptor(v);
  const yearCode = v[9];
  const pos7 = v[6];
  const year = resolveYear(yearCode, pos7);
  const yearStr = year ? String(year) : '';

  // 1. Try local vPIC decode
  if (vinDb) {
    const local = await decodeVinLocal(vinDb, v);
    if (local.make && local.model && local.year) {
      return { decoded: local as VinDecodeResult, source: 'local' };
    }
  }

  // 2. Try API cache (keyed by descriptor + year)
  if (apiCachePath && yearStr) {
    const cached = lookupApiCache(apiCachePath, desc, yearStr);
    if (cached) {
      cached.vin = v;
      return { decoded: cached, source: 'api-cached' };
    }
    apiCacheStats.misses++;
  }

  // 3. Fall back to NHTSA API
  try {
    const resp = await fetch(`https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/${v}?format=json`);
    const data = await resp.json() as { Results: Array<{ Variable: string; Value: string | null }> };
    const results = data.Results || [];

    const getVal = (name: string): string => {
      const r = results.find(d => d.Variable === name);
      return r?.Value && r.Value !== 'Not Applicable' ? r.Value : '';
    };

    const decoded: VinDecodeResult = {
      vin: v,
      make: getVal('Make'),
      model: getVal('Model'),
      year: getVal('Model Year'),
      bodyClass: getVal('Body Class'),
      driveType: getVal('Drive Type'),
      engineCylinders: getVal('Engine Number of Cylinders'),
      displacementL: getVal('Displacement (L)'),
      fuelType: getVal('Fuel Type - Primary'),
      plantCity: getVal('Plant City'),
      plantState: getVal('Plant State'),
      plantCountry: getVal('Plant Country'),
      vehicleType: getVal('Vehicle Type'),
      gvwr: getVal('Gross Vehicle Weight Rating From'),
      manufacturer: getVal('Manufacturer Name'),
      errorCode: getVal('Error Code'),
      errorText: getVal('Error Text'),
      source: 'api',
    };

    // Save to API cache for next time
    if (decoded.make && decoded.model && apiCachePath) {
      saveToApiCache(apiCachePath, desc, decoded);
    }

    return { decoded, source: 'api' };
  } catch {
    return {
      decoded: {
        vin: v,
        make: '', model: '', year: '', bodyClass: '', driveType: '',
        engineCylinders: '', displacementL: '', fuelType: '',
        plantCity: '', plantState: '', plantCountry: '',
        vehicleType: '', gvwr: '', manufacturer: '',
        errorCode: '6', errorText: 'Unable to decode VIN',
        source: 'partial',
      },
      source: 'partial',
    };
  }
}
