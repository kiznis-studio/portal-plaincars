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
CREATE INDEX IF NOT EXISTS idx_recalls_my ON recalls(my_id);
CREATE INDEX IF NOT EXISTS idx_recalls_make ON recalls(make_id);
CREATE INDEX IF NOT EXISTS idx_makes_slug ON makes(slug);
CREATE INDEX IF NOT EXISTS idx_models_slug ON models(slug);
