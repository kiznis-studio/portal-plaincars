// PlainCars D1 query library

// --- Targeted query cache (permanent, for high-frequency stable queries) ---
const queryCache = new Map<string, any>();
export function getQueryCacheSize(): number { return queryCache.size; }
function cached<T>(key: string, compute: () => Promise<T>): Promise<T> {
  if (queryCache.has(key)) return Promise.resolve(queryCache.get(key) as T);
  return compute().then(result => { queryCache.set(key, result); return result; });
}

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
  vin: string | null;
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

export interface ComplaintWithNames extends Complaint {
  make_name?: string;
  model_name?: string;
  year?: number;
}

export interface ComponentSummary {
  component_slug: string;
  component: string;
  complaint_count: number;
  crash_count: number;
  fire_count: number;
  injury_count: number;
  death_count: number;
  affected_makes: number;
  affected_models: number;
  top_models: string | null;
}

export interface YearSummary {
  year: number;
  complaint_count: number;
  crash_count: number;
  fire_count: number;
  injury_count: number;
  death_count: number;
  make_count: number;
  model_count: number;
  top_components: string | null;
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

export interface Investigation {
  nhtsa_id: string;
  subject: string;
  investigation_type: string;
  status: string;
  open_date: string | null;
  latest_activity_date: string | null;
  description: string | null;
  make_id: string | null;
  model_id: string | null;
}

export interface InvestigationWithNames extends Investigation {
  make_name?: string;
  model_name?: string;
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
  return cached('getAllMakes', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM makes ORDER BY complaint_count DESC'
    ).all<Make>();
    return results;
  });
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
  return cached('getNationalStats', async () => {
    try {
      const stat = await db.prepare("SELECT value FROM _stats WHERE key = 'national_stats'").first<{ value: string }>();
      if (stat) return JSON.parse(stat.value) as {
        total_makes: number; total_models: number; total_complaints: number;
        total_recalls: number; total_deaths: number; total_injuries: number;
      };
    } catch { /* _stats may not exist yet */ }
    return db.prepare(`
      SELECT
        (SELECT COUNT(*) FROM makes) as total_makes,
        (SELECT COUNT(*) FROM models) as total_models,
        (SELECT SUM(complaint_count) FROM makes) as total_complaints,
        (SELECT COUNT(*) FROM recalls) as total_recalls,
        (SELECT SUM(death_count) FROM model_years) as total_deaths,
        (SELECT SUM(injury_count) FROM model_years) as total_injuries
    `).first<{
      total_makes: number; total_models: number; total_complaints: number;
      total_recalls: number; total_deaths: number; total_injuries: number;
    }>();
  });
}

// --- Rankings ---

export async function getMostComplainedModels(db: D1Database, limit: number = 100) {
  return cached(`getMostComplainedModels:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT m.*, mk.make_name
      FROM models m JOIN makes mk ON m.make_id = mk.make_id
      ORDER BY m.complaint_count DESC
      LIMIT ?
    `).bind(limit).all();
    return results;
  });
}

export async function getMostDangerousModelYears(db: D1Database, limit: number = 100) {
  return cached(`getMostDangerousModelYears:${limit}`, async () => {
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
  });
}

export async function getMostRecalledModels(db: D1Database, limit: number = 100) {
  return cached(`getMostRecalledModels:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT m.*, mk.make_name
      FROM models m JOIN makes mk ON m.make_id = mk.make_id
      WHERE m.recall_count > 0
      ORDER BY m.recall_count DESC
      LIMIT ?
    `).bind(limit).all();
    return results;
  });
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
  return cached('getAllStates', async () => {
    try {
      const stat = await db.prepare("SELECT value FROM _stats WHERE key = 'state_complaints'").first<{ value: string }>();
      if (stat) {
        const data = JSON.parse(stat.value) as { state: string; count?: number; complaint_count?: number }[];
        return data
          .filter(r => STATE_MAP[r.state])
          .map(r => ({ code: r.state, name: STATE_MAP[r.state].name, slug: STATE_MAP[r.state].slug, complaint_count: r.complaint_count ?? r.count ?? 0 }));
      }
    } catch { /* _stats may not exist yet */ }
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
  });
}

export function getStateBySlug(slug: string): { code: string; name: string; slug: string } | null {
  const code = SLUG_TO_CODE[slug];
  if (!code) return null;
  return { code, name: STATE_MAP[code].name, slug };
}

export async function getMostComplainedInState(db: D1Database, stateCode: string, limit: number = 50): Promise<StateVehicle[]> {
  return cached(`complainedInState:${stateCode}:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT make_name, model_name, model_slug as slug,
             complaint_count, crash_count, fire_count,
             injury_count, death_count, year_min, year_max
      FROM state_top_models
      WHERE state = ?
      ORDER BY complaint_count DESC
      LIMIT ?
    `).bind(stateCode, limit).all<StateVehicle>();
    return results;
  });
}

// --- Safety Ratings (NCAP) ---

export function renderSafetyStars(rating: string | null): string {
  if (!rating) return 'Not Rated';
  const n = parseInt(rating);
  if (isNaN(n) || n < 1 || n > 5) return 'Not Rated';
  return '★'.repeat(n) + '☆'.repeat(5 - n);
}

export async function getTopSafetyRated(db: D1Database, limit = 50): Promise<ModelYearWithNames[]> {
  return cached(`topSafety:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT my.*, mk.make_name, mo.model_name
      FROM model_years my
      JOIN makes mk ON my.make_id = mk.make_id
      JOIN models mo ON my.model_id = mo.model_id
      WHERE my.overall_rating IS NOT NULL AND CAST(my.overall_rating AS INTEGER) >= 4
      ORDER BY CAST(my.overall_rating AS INTEGER) DESC, my.year DESC
      LIMIT ?
    `).bind(limit).all<ModelYearWithNames>();
    return results;
  });
}

export async function getSafetyRatingStats(db: D1Database): Promise<{
  rated_count: number; avg_overall: number; five_star_count: number; four_star_count: number;
}> {
  return cached('safetyStats', async () => {
    const row = await db.prepare(`
      SELECT
        COUNT(*) as rated_count,
        ROUND(AVG(CAST(overall_rating AS REAL)), 1) as avg_overall,
        SUM(CASE WHEN CAST(overall_rating AS INTEGER) = 5 THEN 1 ELSE 0 END) as five_star_count,
        SUM(CASE WHEN CAST(overall_rating AS INTEGER) = 4 THEN 1 ELSE 0 END) as four_star_count
      FROM model_years
      WHERE overall_rating IS NOT NULL
    `).first<{ rated_count: number; avg_overall: number; five_star_count: number; four_star_count: number }>();
    return row ?? { rated_count: 0, avg_overall: 0, five_star_count: 0, four_star_count: 0 };
  });
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
  return cached(`popularModels:${limit}`, async () => {
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
  });
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

// --- Reliability ---

export async function getModelsForReliability(db: D1Database, limit = 500) {
  return cached(`modelsForReliability:${limit}`, async () => {
    const { results } = await db.prepare(`
      SELECT m.model_id, mk.make_name, m.model_name, m.slug,
             COUNT(my.my_id) as year_count,
             MIN(my.year) as first_year, MAX(my.year) as last_year,
             SUM(my.complaint_count) as total_complaints,
             SUM(my.recall_count) as total_recalls,
             SUM(my.crash_count) as total_crashes,
             SUM(my.fire_count) as total_fires,
             SUM(my.injury_count) as total_injuries,
             SUM(my.death_count) as total_deaths
      FROM models m
      JOIN makes mk ON mk.make_id = m.make_id
      JOIN model_years my ON my.model_id = m.model_id
      GROUP BY m.model_id
      HAVING year_count >= 3
      ORDER BY total_complaints ASC
      LIMIT ?
    `).bind(limit).all();
    return results;
  });
}

// --- Search ---

export async function searchModels(db: D1Database, query: string, limit: number = 15) {
  const trimmed = query.trim();
  const like = `${trimmed}%`;
  const { results } = await db.prepare(`
    SELECT m.model_id, m.model_name, m.slug, m.complaint_count, m.year_min, m.year_max, mk.make_name
    FROM models m JOIN makes mk ON m.make_id = mk.make_id
    WHERE m.model_name LIKE ? OR mk.make_name LIKE ? OR (mk.make_name || ' ' || m.model_name) LIKE ?
    ORDER BY m.complaint_count DESC
    LIMIT ?
  `).bind(like, like, like, limit).all();
  return results;
}

// --- Investigations ---

export async function getInvestigationById(db: D1Database, nhtsaId: string): Promise<InvestigationWithNames | null> {
  return db.prepare(`
    SELECT i.*, mk.make_name, mo.model_name
    FROM investigations i
    LEFT JOIN makes mk ON i.make_id = mk.make_id
    LEFT JOIN models mo ON i.model_id = mo.model_id
    WHERE i.nhtsa_id = ?
  `).bind(nhtsaId).first<InvestigationWithNames>();
}

export async function getRecentInvestigations(db: D1Database, limit: number = 50): Promise<InvestigationWithNames[]> {
  const { results } = await db.prepare(`
    SELECT i.nhtsa_id, i.subject, i.investigation_type, i.status, i.open_date, i.latest_activity_date, i.make_id, i.model_id,
           mk.make_name, mo.model_name
    FROM investigations i
    LEFT JOIN makes mk ON i.make_id = mk.make_id
    LEFT JOIN models mo ON i.model_id = mo.model_id
    ORDER BY i.open_date DESC
    LIMIT ?
  `).bind(limit).all<InvestigationWithNames>();
  return results;
}

export async function getOpenInvestigations(db: D1Database, limit: number = 100): Promise<InvestigationWithNames[]> {
  const { results } = await db.prepare(`
    SELECT i.nhtsa_id, i.subject, i.investigation_type, i.status, i.open_date, i.latest_activity_date, i.make_id, i.model_id,
           mk.make_name, mo.model_name
    FROM investigations i
    LEFT JOIN makes mk ON i.make_id = mk.make_id
    LEFT JOIN models mo ON i.model_id = mo.model_id
    WHERE i.status = 'O'
    ORDER BY i.open_date DESC
    LIMIT ?
  `).bind(limit).all<InvestigationWithNames>();
  return results;
}

export async function getInvestigationsByMakeId(db: D1Database, makeId: string): Promise<InvestigationWithNames[]> {
  const { results } = await db.prepare(`
    SELECT i.nhtsa_id, i.subject, i.investigation_type, i.status, i.open_date, i.latest_activity_date, i.make_id, i.model_id,
           mk.make_name, mo.model_name
    FROM investigations i
    LEFT JOIN makes mk ON i.make_id = mk.make_id
    LEFT JOIN models mo ON i.model_id = mo.model_id
    WHERE i.make_id = ?
    ORDER BY i.open_date DESC
  `).bind(makeId).all<InvestigationWithNames>();
  return results;
}

export async function getInvestigationsByModelId(db: D1Database, modelId: string): Promise<InvestigationWithNames[]> {
  const { results } = await db.prepare(`
    SELECT i.nhtsa_id, i.subject, i.investigation_type, i.status, i.open_date, i.latest_activity_date, i.make_id, i.model_id,
           mk.make_name, mo.model_name
    FROM investigations i
    LEFT JOIN makes mk ON i.make_id = mk.make_id
    LEFT JOIN models mo ON i.model_id = mo.model_id
    WHERE i.model_id = ?
    ORDER BY i.open_date DESC
  `).bind(modelId).all<InvestigationWithNames>();
  return results;
}

export async function getInvestigationStats(db: D1Database) {
  return cached('getInvestigationStats', () =>
    db.prepare(`
      SELECT
        COUNT(*) as total,
        SUM(CASE WHEN status = 'O' THEN 1 ELSE 0 END) as open_count,
        SUM(CASE WHEN status = 'C' THEN 1 ELSE 0 END) as closed_count,
        SUM(CASE WHEN investigation_type = 'PE' THEN 1 ELSE 0 END) as pe_count,
        SUM(CASE WHEN investigation_type = 'EA' THEN 1 ELSE 0 END) as ea_count,
        SUM(CASE WHEN investigation_type = 'RQ' THEN 1 ELSE 0 END) as rq_count
      FROM investigations
    `).first<{
      total: number;
      open_count: number;
      closed_count: number;
      pe_count: number;
      ea_count: number;
      rq_count: number;
    }>()
  );
}

// --- Complaint Trends ---

export interface ComplaintTrend {
  cal_year: number;
  complaint_count: number;
  crash_count: number;
  fire_count: number;
  injury_count: number;
  death_count: number;
}

export interface TrendingModel {
  model_id: string;
  make_name: string;
  model_name: string;
  slug: string;
  recent_count: number;
  prev_count: number;
  change_pct: number;
  total_complaints: number;
}

export async function getComplaintTrends(db: D1Database, modelId: string): Promise<ComplaintTrend[]> {
  const { results } = await db.prepare(
    'SELECT cal_year, complaint_count, crash_count, fire_count, injury_count, death_count FROM complaint_trends WHERE model_id = ? ORDER BY cal_year'
  ).bind(modelId).all<ComplaintTrend>();
  return results;
}

export async function getTrendingComplaintModels(db: D1Database, direction: 'rising' | 'falling', limit = 50): Promise<TrendingModel[]> {
  return cached(`trendingModels:${direction}:${limit}`, async () => {
    const order = direction === 'rising' ? 'DESC' : 'ASC';
    const { results } = await db.prepare(`
      SELECT
        r.model_id, mk.make_name, m.model_name, m.slug,
        r.recent_count, p.prev_count,
        ROUND((CAST(r.recent_count AS REAL) - p.prev_count) / MAX(p.prev_count, 1) * 100, 1) as change_pct,
        m.complaint_count as total_complaints
      FROM (
        SELECT model_id, SUM(complaint_count) as recent_count
        FROM complaint_trends WHERE cal_year BETWEEN 2022 AND 2026
        GROUP BY model_id HAVING recent_count >= 5
      ) r
      JOIN (
        SELECT model_id, SUM(complaint_count) as prev_count
        FROM complaint_trends WHERE cal_year BETWEEN 2017 AND 2021
        GROUP BY model_id HAVING prev_count >= 3
      ) p ON r.model_id = p.model_id
      JOIN models m ON m.model_id = r.model_id
      JOIN makes mk ON mk.make_id = m.make_id
      ORDER BY change_pct ${order}
      LIMIT ?
    `).bind(limit).all<TrendingModel>();
    return results;
  });
}

// --- Individual Complaint ---

export async function getComplaintById(db: D1Database, cmplid: number): Promise<ComplaintWithNames | null> {
  return db.prepare(`
    SELECT c.*, mk.make_name, mo.model_name, my.year
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    JOIN models mo ON my.model_id = mo.model_id
    JOIN makes mk ON my.make_id = mk.make_id
    WHERE c.cmplid = ?
  `).bind(cmplid).first<ComplaintWithNames>();
}

export async function getRelatedComplaints(db: D1Database, myId: string, component: string, excludeCmplid: number, limit = 10): Promise<Complaint[]> {
  const { results } = await db.prepare(`
    SELECT * FROM complaints
    WHERE my_id = ? AND component = ? AND cmplid != ?
    ORDER BY date_added DESC LIMIT ?
  `).bind(myId, component, excludeCmplid, limit).all<Complaint>();
  return results;
}

// --- VIN Lookup ---

export async function getComplaintsByVin(db: D1Database, vin: string): Promise<ComplaintWithNames[]> {
  // NHTSA complaints store truncated 11-char VINs (no serial number)
  const vinTruncated = vin.toUpperCase().slice(0, 11);
  const { results } = await db.prepare(`
    SELECT c.*, mk.make_name, mo.model_name, my.year
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    JOIN models mo ON my.model_id = mo.model_id
    JOIN makes mk ON my.make_id = mk.make_id
    WHERE c.vin = ?
    ORDER BY c.date_added DESC
  `).bind(vinTruncated).all<ComplaintWithNames>();
  return results;
}

// --- Component Pages ---

export async function getAllComponents(db: D1Database): Promise<ComponentSummary[]> {
  return cached('getAllComponents', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM component_summary ORDER BY complaint_count DESC'
    ).all<ComponentSummary>();
    return results;
  });
}

export function getRelatedComponents(
  db: D1Database, excludeSlug: string, limit = 6
): Promise<Pick<ComponentSummary, 'component_slug' | 'component' | 'complaint_count' | 'death_count' | 'affected_models'>[]> {
  return cached(`relatedComponents:${excludeSlug}:${limit}`, async () => {
    const { results } = await db.prepare(
      `SELECT component_slug, component, complaint_count, death_count, affected_models
       FROM component_summary WHERE component_slug != ?
       ORDER BY complaint_count DESC LIMIT ?`
    ).bind(excludeSlug, limit).all();
    return results as any[];
  });
}

export async function getComponentBySlug(db: D1Database, slug: string): Promise<ComponentSummary | null> {
  return db.prepare(
    'SELECT * FROM component_summary WHERE component_slug = ?'
  ).bind(slug).first<ComponentSummary>();
}

export async function getComponentTopModels(db: D1Database, component: string, limit = 30) {
  const { results } = await db.prepare(`
    SELECT make_name, model_name, model_slug as slug,
           complaint_count, crash_count, fire_count,
           injury_count, death_count
    FROM component_top_models
    WHERE component = ?
    ORDER BY complaint_count DESC
    LIMIT ?
  `).bind(component, limit).all();
  return results;
}

export async function getComponentRecentComplaints(db: D1Database, component: string, limit = 20): Promise<ComplaintWithNames[]> {
  const { results } = await db.prepare(`
    SELECT c.*, mk.make_name, mo.model_name, my.year
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    JOIN models mo ON my.model_id = mo.model_id
    JOIN makes mk ON my.make_id = mk.make_id
    WHERE c.component = ?
    ORDER BY c.date_added DESC
    LIMIT ?
  `).bind(component, limit).all<ComplaintWithNames>();
  return results;
}

// --- Year Pages ---

export async function getAllYearSummaries(db: D1Database): Promise<YearSummary[]> {
  return cached('getAllYearSummaries', async () => {
    const { results } = await db.prepare(
      'SELECT * FROM year_summary ORDER BY year DESC'
    ).all<YearSummary>();
    return results;
  });
}

export async function getYearSummary(db: D1Database, year: number): Promise<YearSummary | null> {
  return db.prepare(
    'SELECT * FROM year_summary WHERE year = ?'
  ).bind(year).first<YearSummary>();
}

export async function getYearTopModels(db: D1Database, year: number, limit = 30) {
  const { results } = await db.prepare(`
    SELECT my.*, mk.make_name, mo.model_name, mo.slug
    FROM model_years my
    JOIN makes mk ON my.make_id = mk.make_id
    JOIN models mo ON my.model_id = mo.model_id
    WHERE my.year = ?
    ORDER BY my.complaint_count DESC
    LIMIT ?
  `).bind(year, limit).all();
  return results;
}

export async function getYearTopComponents(db: D1Database, year: number, limit = 20) {
  const { results } = await db.prepare(`
    SELECT component, complaint_count, crash_count, fire_count
    FROM year_top_components
    WHERE year = ?
    ORDER BY complaint_count DESC
    LIMIT ?
  `).bind(year, limit).all();
  return results;
}

// --- Recent / Browse ---

export async function getRecentComplaints(db: D1Database, limit = 50): Promise<ComplaintWithNames[]> {
  const { results } = await db.prepare(`
    SELECT c.*, mk.make_name, mo.model_name, my.year
    FROM complaints c
    JOIN model_years my ON c.my_id = my.my_id
    JOIN models mo ON my.model_id = mo.model_id
    JOIN makes mk ON my.make_id = mk.make_id
    ORDER BY c.date_added DESC
    LIMIT ?
  `).bind(limit).all<ComplaintWithNames>();
  return results;
}

export const INVESTIGATION_TYPES: Record<string, string> = {
  PE: 'Preliminary Evaluation',
  EA: 'Engineering Analysis',
  RQ: 'Recall Query',
  DP: 'Defect Petition',
  AQ: 'Audit Query',
};

export async function warmQueryCache(db: D1Database): Promise<number> {
  const start = Date.now();
  const states = await getAllStates(db);
  await Promise.all([
    getAllMakes(db),
    getNationalStats(db),
    getMostComplainedModels(db),
    getMostDangerousModelYears(db),
    getMostRecalledModels(db),
    getInvestigationStats(db),
    getTopSafetyRated(db),
    getSafetyRatingStats(db),
    getPopularModels(db),
    getModelsForReliability(db),
    getTrendingComplaintModels(db, 'rising'),
    getTrendingComplaintModels(db, 'falling'),
    getAllComponents(db),
    getAllYearSummaries(db),
    ...states.map(s => getMostComplainedInState(db, s.code)),
  ]);
  console.log(`[cache] Warmed ${queryCache.size} queries in ${Date.now() - start}ms`);
  return queryCache.size;
}
