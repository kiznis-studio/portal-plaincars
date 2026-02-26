// PlainCars D1 query library

export interface Make {
  make_id: string;
  make_name: string;
  slug: string;
  complaint_count: number;
  recall_count: number;
  model_count: number;
}

export interface Model {
  model_id: string;
  make_id: string;
  model_name: string;
  slug: string;
  year_min: number;
  year_max: number;
  complaint_count: number;
  recall_count: number;
}

export interface ModelYear {
  my_id: string;
  model_id: string;
  make_id: string;
  year: number;
  complaint_count: number;
  crash_count: number;
  fire_count: number;
  injury_count: number;
  death_count: number;
  recall_count: number;
  overall_rating: string | null;
  front_crash_rating: string | null;
  side_crash_rating: string | null;
  rollover_rating: string | null;
  rollover_risk: number | null;
}

export interface ComplaintStat {
  cs_id: number;
  my_id: string;
  component: string;
  complaint_count: number;
  crash_count: number;
  fire_count: number;
  injury_count: number;
  death_count: number;
  sample_text: string;
}

export interface Complaint {
  cmplid: number;
  my_id: string;
  odi_number: number;
  crash: string;
  fire: string;
  injured: number;
  deaths: number;
  component: string;
  summary: string;
  fail_date: string;
  date_added: string;
  mileage: number | null;
  state: string;
}

export interface Recall {
  recall_id: string;
  campaign_number: string;
  my_id: string | null;
  make_id: string;
  model_id: string | null;
  year: number;
  component: string;
  summary: string;
  consequence: string;
  remedy: string;
  report_date: string;
}

export interface ModelWithMake extends Model {
  make_name?: string;
}

export interface ModelYearWithNames extends ModelYear {
  make_name?: string;
  model_name?: string;
}

// --- Makes ---

export async function getAllMakes(db: D1Database): Promise<Make[]> {
  const { results } = await db.prepare(
    'SELECT * FROM makes ORDER BY complaint_count DESC'
  ).all<Make>();
  return results;
}

export async function getMakeBySlug(db: D1Database, slug: string): Promise<Make | null> {
  return db.prepare('SELECT * FROM makes WHERE slug = ?').bind(slug).first<Make>();
}

// --- Models ---

export async function getModelsByMake(db: D1Database, makeId: string): Promise<Model[]> {
  const { results } = await db.prepare(
    'SELECT * FROM models WHERE make_id = ? ORDER BY complaint_count DESC'
  ).bind(makeId).all<Model>();
  return results;
}

export async function getModelBySlug(db: D1Database, slug: string): Promise<ModelWithMake | null> {
  return db.prepare(`
    SELECT m.*, mk.make_name
    FROM models m JOIN makes mk ON m.make_id = mk.make_id
    WHERE m.slug = ?
  `).bind(slug).first<ModelWithMake>();
}

// --- Model Years ---

export async function getModelYears(db: D1Database, modelId: string): Promise<ModelYear[]> {
  const { results } = await db.prepare(
    'SELECT * FROM model_years WHERE model_id = ? ORDER BY year DESC'
  ).bind(modelId).all<ModelYear>();
  return results;
}

export async function getModelYearById(db: D1Database, myId: string): Promise<ModelYearWithNames | null> {
  return db.prepare(`
    SELECT my.*, mk.make_name, mo.model_name
    FROM model_years my
    JOIN makes mk ON my.make_id = mk.make_id
    JOIN models mo ON my.model_id = mo.model_id
    WHERE my.my_id = ?
  `).bind(myId).first<ModelYearWithNames>();
}

// --- Complaint Stats ---

export async function getComplaintStats(db: D1Database, myId: string): Promise<ComplaintStat[]> {
  const { results } = await db.prepare(
    'SELECT * FROM complaint_stats WHERE my_id = ? ORDER BY complaint_count DESC'
  ).bind(myId).all<ComplaintStat>();
  return results;
}

// --- Complaints ---

export async function getComplaints(db: D1Database, myId: string, limit: number = 50): Promise<Complaint[]> {
  const { results } = await db.prepare(
    'SELECT * FROM complaints WHERE my_id = ? ORDER BY date_added DESC LIMIT ?'
  ).bind(myId, limit).all<Complaint>();
  return results;
}

// --- Recalls ---

export async function getRecallsByModelYear(db: D1Database, myId: string): Promise<Recall[]> {
  const { results } = await db.prepare(
    'SELECT * FROM recalls WHERE my_id = ? ORDER BY report_date DESC'
  ).bind(myId).all<Recall>();
  return results;
}

export async function getRecallsByMake(db: D1Database, makeId: string, limit: number = 50): Promise<Recall[]> {
  const { results } = await db.prepare(
    'SELECT * FROM recalls WHERE make_id = ? ORDER BY report_date DESC LIMIT ?'
  ).bind(makeId, limit).all<Recall>();
  return results;
}

export async function getRecentRecalls(db: D1Database, limit: number = 50): Promise<Recall[]> {
  const { results } = await db.prepare(
    'SELECT * FROM recalls ORDER BY report_date DESC LIMIT ?'
  ).bind(limit).all<Recall>();
  return results;
}

// --- Stats ---

export async function getNationalStats(db: D1Database) {
  return db.prepare(`
    SELECT
      (SELECT COUNT(*) FROM makes) as total_makes,
      (SELECT COUNT(*) FROM models) as total_models,
      (SELECT SUM(complaint_count) FROM makes) as total_complaints,
      (SELECT COUNT(*) FROM recalls) as total_recalls,
      (SELECT SUM(death_count) FROM model_years) as total_deaths,
      (SELECT SUM(injury_count) FROM model_years) as total_injuries
  `).first<{
    total_makes: number;
    total_models: number;
    total_complaints: number;
    total_recalls: number;
    total_deaths: number;
    total_injuries: number;
  }>();
}

// --- Rankings ---

export async function getMostComplainedModels(db: D1Database, limit: number = 100) {
  const { results } = await db.prepare(`
    SELECT m.*, mk.make_name
    FROM models m JOIN makes mk ON m.make_id = mk.make_id
    ORDER BY m.complaint_count DESC
    LIMIT ?
  `).bind(limit).all();
  return results;
}

export async function getMostDangerousModelYears(db: D1Database, limit: number = 100) {
  const { results } = await db.prepare(`
    SELECT my.*, mk.make_name, mo.model_name
    FROM model_years my
    JOIN makes mk ON my.make_id = mk.make_id
    JOIN models mo ON my.model_id = mo.model_id
    WHERE my.death_count > 0 OR my.crash_count > 10
    ORDER BY my.death_count DESC, my.crash_count DESC
    LIMIT ?
  `).bind(limit).all();
  return results;
}

export async function getMostRecalledModels(db: D1Database, limit: number = 100) {
  const { results } = await db.prepare(`
    SELECT m.*, mk.make_name
    FROM models m JOIN makes mk ON m.make_id = mk.make_id
    WHERE m.recall_count > 0
    ORDER BY m.recall_count DESC
    LIMIT ?
  `).bind(limit).all();
  return results;
}

// --- State Map ---

export const STATE_MAP: Record<string, { name: string; slug: string }> = {
  AL: { name: 'Alabama', slug: 'alabama' },
  AK: { name: 'Alaska', slug: 'alaska' },
  AZ: { name: 'Arizona', slug: 'arizona' },
  AR: { name: 'Arkansas', slug: 'arkansas' },
  CA: { name: 'California', slug: 'california' },
  CO: { name: 'Colorado', slug: 'colorado' },
  CT: { name: 'Connecticut', slug: 'connecticut' },
  DE: { name: 'Delaware', slug: 'delaware' },
  DC: { name: 'District of Columbia', slug: 'district-of-columbia' },
  FL: { name: 'Florida', slug: 'florida' },
  GA: { name: 'Georgia', slug: 'georgia' },
  HI: { name: 'Hawaii', slug: 'hawaii' },
  ID: { name: 'Idaho', slug: 'idaho' },
  IL: { name: 'Illinois', slug: 'illinois' },
  IN: { name: 'Indiana', slug: 'indiana' },
  IA: { name: 'Iowa', slug: 'iowa' },
  KS: { name: 'Kansas', slug: 'kansas' },
  KY: { name: 'Kentucky', slug: 'kentucky' },
  LA: { name: 'Louisiana', slug: 'louisiana' },
  ME: { name: 'Maine', slug: 'maine' },
  MD: { name: 'Maryland', slug: 'maryland' },
  MA: { name: 'Massachusetts', slug: 'massachusetts' },
  MI: { name: 'Michigan', slug: 'michigan' },
  MN: { name: 'Minnesota', slug: 'minnesota' },
  MS: { name: 'Mississippi', slug: 'mississippi' },
  MO: { name: 'Missouri', slug: 'missouri' },
  MT: { name: 'Montana', slug: 'montana' },
  NE: { name: 'Nebraska', slug: 'nebraska' },
  NV: { name: 'Nevada', slug: 'nevada' },
  NH: { name: 'New Hampshire', slug: 'new-hampshire' },
  NJ: { name: 'New Jersey', slug: 'new-jersey' },
  NM: { name: 'New Mexico', slug: 'new-mexico' },
  NY: { name: 'New York', slug: 'new-york' },
  NC: { name: 'North Carolina', slug: 'north-carolina' },
  ND: { name: 'North Dakota', slug: 'north-dakota' },
  OH: { name: 'Ohio', slug: 'ohio' },
  OK: { name: 'Oklahoma', slug: 'oklahoma' },
  OR: { name: 'Oregon', slug: 'oregon' },
  PA: { name: 'Pennsylvania', slug: 'pennsylvania' },
  RI: { name: 'Rhode Island', slug: 'rhode-island' },
  SC: { name: 'South Carolina', slug: 'south-carolina' },
  SD: { name: 'South Dakota', slug: 'south-dakota' },
  TN: { name: 'Tennessee', slug: 'tennessee' },
  TX: { name: 'Texas', slug: 'texas' },
  UT: { name: 'Utah', slug: 'utah' },
  VT: { name: 'Vermont', slug: 'vermont' },
  VA: { name: 'Virginia', slug: 'virginia' },
  WA: { name: 'Washington', slug: 'washington' },
  WV: { name: 'West Virginia', slug: 'west-virginia' },
  WI: { name: 'Wisconsin', slug: 'wisconsin' },
  WY: { name: 'Wyoming', slug: 'wyoming' },
};

// Reverse lookup: slug → state code
const SLUG_TO_CODE: Record<string, string> = Object.fromEntries(
  Object.entries(STATE_MAP).map(([code, { slug }]) => [slug, code])
);

export interface StateInfo {
  code: string;
  name: string;
  slug: string;
  complaint_count: number;
}

export interface StateVehicle {
  make_name: string;
  model_name: string;
  slug: string;
  complaint_count: number;
  crash_count: number;
  fire_count: number;
  injury_count: number;
  death_count: number;
  year_min: number;
  year_max: number;
}

export async function getAllStates(db: D1Database): Promise<StateInfo[]> {
  const { results } = await db.prepare(
    `SELECT state, COUNT(*) as complaint_count
     FROM complaints
     WHERE state IS NOT NULL AND state != ''
     GROUP BY state
     ORDER BY complaint_count DESC`
  ).all<{ state: string; complaint_count: number }>();

  return results
    .filter(r => STATE_MAP[r.state])
    .map(r => ({
      code: r.state,
      name: STATE_MAP[r.state].name,
      slug: STATE_MAP[r.state].slug,
      complaint_count: r.complaint_count,
    }));
}

export function getStateBySlug(slug: string): { code: string; name: string; slug: string } | null {
  const code = SLUG_TO_CODE[slug];
  if (!code) return null;
  return { code, name: STATE_MAP[code].name, slug };
}

export async function getMostComplainedInState(db: D1Database, stateCode: string, limit: number = 50): Promise<StateVehicle[]> {
  const { results } = await db.prepare(`
    SELECT
      mk.make_name, mo.model_name, mo.slug,
      COUNT(*) as complaint_count,
      SUM(CASE WHEN c.crash = 'Y' THEN 1 ELSE 0 END) as crash_count,
      SUM(CASE WHEN c.fire = 'Y' THEN 1 ELSE 0 END) as fire_count,
      SUM(c.injured) as injury_count,
      SUM(c.deaths) as death_count,
      MIN(my.year) as year_min,
      MAX(my.year) as year_max
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    JOIN models mo ON my.model_id = mo.model_id
    JOIN makes mk ON my.make_id = mk.make_id
    WHERE c.state = ?
    GROUP BY mo.model_id
    ORDER BY complaint_count DESC
    LIMIT ?
  `).bind(stateCode, limit).all<StateVehicle>();
  return results;
}

// --- Compare ---

export interface ModelAggregateStats {
  model_id: string;
  make_name: string;
  model_name: string;
  slug: string;
  year_count: number;
  first_year: number;
  last_year: number;
  avg_rating: number | null;
  total_complaints: number;
  total_recalls: number;
  total_crashes: number;
  total_fires: number;
  total_injuries: number;
  total_deaths: number;
}

export async function getModelAggregateStats(db: D1Database, modelId: string): Promise<ModelAggregateStats | null> {
  return db.prepare(`
    SELECT
      m.model_id, mk.make_name, m.model_name, m.slug,
      COUNT(DISTINCT my.year) as year_count,
      MIN(my.year) as first_year, MAX(my.year) as last_year,
      ROUND(AVG(CASE WHEN my.overall_rating IS NOT NULL THEN CAST(my.overall_rating AS REAL) END), 1) as avg_rating,
      SUM(my.complaint_count) as total_complaints,
      SUM(my.recall_count) as total_recalls,
      SUM(my.crash_count) as total_crashes,
      SUM(my.fire_count) as total_fires,
      SUM(my.injury_count) as total_injuries,
      SUM(my.death_count) as total_deaths
    FROM models m
    JOIN makes mk ON m.make_id = mk.make_id
    JOIN model_years my ON my.model_id = m.model_id
    WHERE m.model_id = ?
    GROUP BY m.model_id
  `).bind(modelId).first<ModelAggregateStats>();
}

export async function getModelTopComplaints(db: D1Database, modelId: string, limit: number = 5) {
  const { results } = await db.prepare(`
    SELECT component, SUM(complaint_count) as count
    FROM complaint_stats
    WHERE my_id IN (SELECT my_id FROM model_years WHERE model_id = ?)
    GROUP BY component
    ORDER BY count DESC
    LIMIT ?
  `).bind(modelId, limit).all<{ component: string; count: number }>();
  return results;
}

export async function getPopularModels(db: D1Database, limit: number = 50) {
  const { results } = await db.prepare(`
    SELECT m.model_id, mk.make_name, m.model_name, m.slug,
           COUNT(my.my_id) as year_count, m.complaint_count as total_complaints
    FROM models m
    JOIN makes mk ON m.make_id = mk.make_id
    JOIN model_years my ON my.model_id = m.model_id
    GROUP BY m.model_id
    ORDER BY year_count DESC, m.complaint_count DESC
    LIMIT ?
  `).bind(limit).all();
  return results;
}

export async function getCompareModelsForModel(db: D1Database, modelId: string, limit: number = 5) {
  const { results } = await db.prepare(`
    SELECT m.model_id, mk.make_name, m.model_name, m.slug, m.complaint_count
    FROM models m
    JOIN makes mk ON m.make_id = mk.make_id
    WHERE m.model_id != ?
    ORDER BY m.complaint_count DESC
    LIMIT ?
  `).bind(modelId, limit).all();
  return results;
}

// --- Search ---

export async function searchModels(db: D1Database, query: string, limit: number = 15) {
  const like = `%${query.trim()}%`;
  const { results } = await db.prepare(`
    SELECT m.model_id, m.model_name, m.slug, m.complaint_count, m.year_min, m.year_max, mk.make_name
    FROM models m JOIN makes mk ON m.make_id = mk.make_id
    WHERE m.model_name LIKE ? OR mk.make_name LIKE ? OR (mk.make_name || ' ' || m.model_name) LIKE ?
    ORDER BY m.complaint_count DESC
    LIMIT ?
  `).bind(like, like, like, limit).all();
  return results;
}
