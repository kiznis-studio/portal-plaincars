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
