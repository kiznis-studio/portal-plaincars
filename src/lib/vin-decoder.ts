/**
 * Local VIN Decoder — uses NHTSA vPIC SQLite DB for offline VIN decoding.
 *
 * Decodes VIN → make, model, year, body class, drive type, engine specs, plant info.
 *
 * Priority chain:
 *   1. WMI table → Make & Manufacturer (always works, chars 1-3)
 *   2. Year code → Model Year (always works, char 10)
 *   3. Pattern matching → Body, Drive, Engine, Plant (when pattern matches)
 *   4. NHTSA API fallback → full decode (only when local fails)
 *
 * The vin-decoder.db is mounted alongside plaincars.db in the Docker container.
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
// Position 7 is numeric for cycle 2 (2010+), alpha for cycle 1 (1980-2009)
function resolveYear(yearCode: string, pos7: string): number {
  const years = YEAR_CODES[yearCode.toUpperCase()];
  if (!years) return 0;
  if (years.length === 1) return years[0];
  // If position 7 is a digit, it's cycle 2 (2010+)
  // If position 7 is a letter, it's cycle 1 (1980-2009)
  return /\d/.test(pos7) ? years[1] : years[0];
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

/**
 * Full VIN decode with local-first, API-fallback strategy.
 *
 * Returns the same shape as the NHTSA API response but decoded locally
 * when possible. Falls back to NHTSA API only when local decode is insufficient.
 */
export async function decodeVin(
  vinDb: any | null,
  vin: string
): Promise<{ decoded: VinDecodeResult; source: 'local' | 'api' | 'partial' }> {
  const v = vin.toUpperCase();

  // Try local decode first
  if (vinDb) {
    const local = await decodeVinLocal(vinDb, v);
    if (local.make && local.model && local.year) {
      return {
        decoded: local as VinDecodeResult,
        source: 'local',
      };
    }
  }

  // Fall back to NHTSA API
  try {
    const resp = await fetch(`https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVin/${v}?format=json`);
    const data = await resp.json() as { Results: Array<{ Variable: string; Value: string | null }> };
    const results = data.Results || [];

    const getVal = (name: string): string => {
      const r = results.find(d => d.Variable === name);
      return r?.Value && r.Value !== 'Not Applicable' ? r.Value : '';
    };

    return {
      decoded: {
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
      },
      source: 'api',
    };
  } catch {
    // Both failed — return whatever local decode had
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
