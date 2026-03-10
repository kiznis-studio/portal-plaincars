-- PlainCars D1 Schema — NHTSA Vehicle Safety Data

CREATE TABLE IF NOT EXISTS makes (
  make_id TEXT PRIMARY KEY,
  make_name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  complaint_count INTEGER DEFAULT 0,
  recall_count INTEGER DEFAULT 0,
  model_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS models (
  model_id TEXT PRIMARY KEY,
  make_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  slug TEXT NOT NULL UNIQUE,
  year_min INTEGER,
  year_max INTEGER,
  complaint_count INTEGER DEFAULT 0,
  recall_count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS model_years (
  my_id TEXT PRIMARY KEY,
  model_id TEXT NOT NULL,
  make_id TEXT NOT NULL,
  year INTEGER NOT NULL,
  complaint_count INTEGER DEFAULT 0,
  crash_count INTEGER DEFAULT 0,
  fire_count INTEGER DEFAULT 0,
  injury_count INTEGER DEFAULT 0,
  death_count INTEGER DEFAULT 0,
  recall_count INTEGER DEFAULT 0,
  overall_rating TEXT,
  front_crash_rating TEXT,
  side_crash_rating TEXT,
  rollover_rating TEXT,
  rollover_risk REAL
);

CREATE TABLE IF NOT EXISTS complaint_stats (
  cs_id INTEGER PRIMARY KEY AUTOINCREMENT,
  my_id TEXT NOT NULL,
  component TEXT NOT NULL,
  complaint_count INTEGER DEFAULT 0,
  crash_count INTEGER DEFAULT 0,
  fire_count INTEGER DEFAULT 0,
  injury_count INTEGER DEFAULT 0,
  death_count INTEGER DEFAULT 0,
  sample_text TEXT
);

CREATE TABLE IF NOT EXISTS complaints (
  cmplid INTEGER PRIMARY KEY,
  my_id TEXT NOT NULL,
  odi_number INTEGER,
  vin TEXT,
  crash TEXT,
  fire TEXT,
  injured INTEGER DEFAULT 0,
  deaths INTEGER DEFAULT 0,
  component TEXT,
  summary TEXT,
  fail_date TEXT,
  date_added TEXT,
  mileage INTEGER,
  state TEXT
);

CREATE TABLE IF NOT EXISTS recalls (
  recall_id TEXT PRIMARY KEY,
  campaign_number TEXT NOT NULL,
  my_id TEXT,
  make_id TEXT,
  model_id TEXT,
  year INTEGER,
  component TEXT,
  summary TEXT,
  consequence TEXT,
  remedy TEXT,
  report_date TEXT,
  affected_count INTEGER
);

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

-- Pre-computed: complaint trends by model and calendar year
CREATE TABLE IF NOT EXISTS complaint_trends (
  model_id TEXT NOT NULL,
  cal_year INTEGER NOT NULL,
  complaint_count INTEGER DEFAULT 0,
  crash_count INTEGER DEFAULT 0,
  fire_count INTEGER DEFAULT 0,
  injury_count INTEGER DEFAULT 0,
  death_count INTEGER DEFAULT 0,
  PRIMARY KEY (model_id, cal_year)
);

-- Pre-computed: component-level stats across all vehicles
CREATE TABLE IF NOT EXISTS component_summary (
  component_slug TEXT PRIMARY KEY,
  component TEXT NOT NULL,
  complaint_count INTEGER DEFAULT 0,
  crash_count INTEGER DEFAULT 0,
  fire_count INTEGER DEFAULT 0,
  injury_count INTEGER DEFAULT 0,
  death_count INTEGER DEFAULT 0,
  affected_makes INTEGER DEFAULT 0,
  affected_models INTEGER DEFAULT 0,
  top_models TEXT
);

-- Pre-computed: model year summary stats
CREATE TABLE IF NOT EXISTS year_summary (
  year INTEGER PRIMARY KEY,
  complaint_count INTEGER DEFAULT 0,
  crash_count INTEGER DEFAULT 0,
  fire_count INTEGER DEFAULT 0,
  injury_count INTEGER DEFAULT 0,
  death_count INTEGER DEFAULT 0,
  make_count INTEGER DEFAULT 0,
  model_count INTEGER DEFAULT 0,
  top_components TEXT
);

CREATE TABLE IF NOT EXISTS _stats (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_inv_make ON investigations(make_id);
CREATE INDEX IF NOT EXISTS idx_inv_model ON investigations(model_id);
CREATE INDEX IF NOT EXISTS idx_inv_status ON investigations(status);
CREATE INDEX IF NOT EXISTS idx_inv_type ON investigations(investigation_type);
CREATE INDEX IF NOT EXISTS idx_models_make ON models(make_id);
CREATE INDEX IF NOT EXISTS idx_model_years_model ON model_years(model_id);
CREATE INDEX IF NOT EXISTS idx_model_years_make ON model_years(make_id);
CREATE INDEX IF NOT EXISTS idx_model_years_year ON model_years(year);
CREATE INDEX IF NOT EXISTS idx_complaint_stats_my ON complaint_stats(my_id);
CREATE INDEX IF NOT EXISTS idx_complaints_my ON complaints(my_id);
CREATE INDEX IF NOT EXISTS idx_complaints_vin ON complaints(vin) WHERE vin IS NOT NULL AND vin != '';
CREATE INDEX IF NOT EXISTS idx_complaints_component ON complaints(component);
CREATE INDEX IF NOT EXISTS idx_complaints_date ON complaints(date_added DESC);
CREATE INDEX IF NOT EXISTS idx_complaints_state ON complaints(state);
CREATE INDEX IF NOT EXISTS idx_complaint_trends_model ON complaint_trends(model_id, cal_year);
CREATE INDEX IF NOT EXISTS idx_component_summary_count ON component_summary(complaint_count DESC);
CREATE INDEX IF NOT EXISTS idx_year_summary_year ON year_summary(year DESC);
CREATE INDEX IF NOT EXISTS idx_recalls_my ON recalls(my_id);
CREATE INDEX IF NOT EXISTS idx_recalls_make ON recalls(make_id);
CREATE INDEX IF NOT EXISTS idx_makes_slug ON makes(slug);
CREATE INDEX IF NOT EXISTS idx_models_slug ON models(slug);
