#!/bin/bash
# optimize-db.sh — Add pre-computed cache tables to plaincars.db
# Run after data import: bash scripts/optimize-db.sh data/plaincars.db
#
# Creates materialized tables that replace expensive GROUP BY queries on the
# 2M-row complaints table. Drops warmup time from ~136s to <5s.

set -euo pipefail

DB="${1:?Usage: optimize-db.sh <path-to-plaincars.db>}"

if [ ! -f "$DB" ]; then
  echo "ERROR: Database not found at $DB"
  exit 1
fi

echo "Optimizing $DB..."
echo "DB size before: $(du -h "$DB" | cut -f1)"

# Step 1: state_top_models — replaces getMostComplainedInState GROUP BY
# Previously: 51 states × ~2s per GROUP BY on 2M complaints = 136s warmup
# Now: flat lookup on ~2,750 rows = instant
echo "Step 1/3: Creating state_top_models..."
sqlite3 "$DB" <<'SQL'
DROP TABLE IF EXISTS state_top_models;
CREATE TABLE state_top_models (
  state TEXT NOT NULL,
  make_name TEXT NOT NULL,
  model_name TEXT NOT NULL,
  model_slug TEXT NOT NULL,
  complaint_count INTEGER NOT NULL,
  crash_count INTEGER NOT NULL DEFAULT 0,
  fire_count INTEGER NOT NULL DEFAULT 0,
  injury_count INTEGER NOT NULL DEFAULT 0,
  death_count INTEGER NOT NULL DEFAULT 0,
  year_min INTEGER,
  year_max INTEGER
);

INSERT INTO state_top_models
SELECT state, make_name, model_name, model_slug, complaint_count,
       crash_count, fire_count, injury_count, death_count, year_min, year_max
FROM (
  SELECT
    c.state,
    mk.make_name,
    mo.model_name,
    mo.slug AS model_slug,
    COUNT(*) AS complaint_count,
    SUM(CASE WHEN c.crash = 'Y' THEN 1 ELSE 0 END) AS crash_count,
    SUM(CASE WHEN c.fire = 'Y' THEN 1 ELSE 0 END) AS fire_count,
    SUM(c.injured) AS injury_count,
    SUM(c.deaths) AS death_count,
    MIN(my.year) AS year_min,
    MAX(my.year) AS year_max,
    ROW_NUMBER() OVER (PARTITION BY c.state ORDER BY COUNT(*) DESC) AS rn
  FROM complaints c
  JOIN model_years my ON c.my_id = my.my_id
  JOIN models mo ON my.model_id = mo.model_id
  JOIN makes mk ON my.make_id = mk.make_id
  WHERE c.state IS NOT NULL AND c.state <> ''
  GROUP BY c.state, mo.model_id
) sub
WHERE rn <= 50;

CREATE INDEX idx_stm_state ON state_top_models(state, complaint_count DESC);
SQL
echo "  $(sqlite3 "$DB" 'SELECT COUNT(*) FROM state_top_models') rows"

# Step 2: component_top_models — replaces getComponentTopModels GROUP BY
# Previously: GROUP BY on 2M complaints × 3 JOINs per component = 11.3s cold
# Now: flat lookup = <5ms
echo "Step 2/3: Creating component_top_models..."
sqlite3 "$DB" <<'SQL'
DROP TABLE IF EXISTS component_top_models;
CREATE TABLE component_top_models (
  component TEXT NOT NULL,
  make_name TEXT NOT NULL,
  model_name TEXT NOT NULL,
  model_slug TEXT NOT NULL,
  complaint_count INTEGER NOT NULL,
  crash_count INTEGER NOT NULL DEFAULT 0,
  fire_count INTEGER NOT NULL DEFAULT 0,
  injury_count INTEGER NOT NULL DEFAULT 0,
  death_count INTEGER NOT NULL DEFAULT 0
);

INSERT INTO component_top_models
SELECT component, make_name, model_name, model_slug, complaint_count,
       crash_count, fire_count, injury_count, death_count
FROM (
  SELECT
    c.component,
    mk.make_name,
    mo.model_name,
    mo.slug AS model_slug,
    COUNT(*) AS complaint_count,
    SUM(CASE WHEN c.crash = 'Y' THEN 1 ELSE 0 END) AS crash_count,
    SUM(CASE WHEN c.fire = 'Y' THEN 1 ELSE 0 END) AS fire_count,
    SUM(c.injured) AS injury_count,
    SUM(c.deaths) AS death_count,
    ROW_NUMBER() OVER (PARTITION BY c.component ORDER BY COUNT(*) DESC) AS rn
  FROM complaints c
  JOIN model_years my ON c.my_id = my.my_id
  JOIN models mo ON my.model_id = mo.model_id
  JOIN makes mk ON my.make_id = mk.make_id
  GROUP BY c.component, mo.model_id
) sub
WHERE rn <= 30;

CREATE INDEX idx_ctm_component ON component_top_models(component, complaint_count DESC);
SQL
echo "  $(sqlite3 "$DB" 'SELECT COUNT(*) FROM component_top_models') rows"

# Step 3: year_top_components — replaces getYearTopComponents GROUP BY
# Previously: GROUP BY complaints × JOIN model_years per year page
# Now: flat lookup = <5ms
echo "Step 3/3: Creating year_top_components..."
sqlite3 "$DB" <<'SQL'
DROP TABLE IF EXISTS year_top_components;
CREATE TABLE year_top_components (
  year INTEGER NOT NULL,
  component TEXT NOT NULL,
  complaint_count INTEGER NOT NULL,
  crash_count INTEGER NOT NULL DEFAULT 0,
  fire_count INTEGER NOT NULL DEFAULT 0
);

INSERT INTO year_top_components
SELECT year, component, complaint_count, crash_count, fire_count
FROM (
  SELECT
    my.year,
    c.component,
    COUNT(*) AS complaint_count,
    SUM(CASE WHEN c.crash = 'Y' THEN 1 ELSE 0 END) AS crash_count,
    SUM(CASE WHEN c.fire = 'Y' THEN 1 ELSE 0 END) AS fire_count,
    ROW_NUMBER() OVER (PARTITION BY my.year ORDER BY COUNT(*) DESC) AS rn
  FROM complaints c
  JOIN model_years my ON c.my_id = my.my_id
  GROUP BY my.year, c.component
) sub
WHERE rn <= 20;

CREATE INDEX idx_ytc_year ON year_top_components(year, complaint_count DESC);
SQL
echo "  $(sqlite3 "$DB" 'SELECT COUNT(*) FROM year_top_components') rows"

# Final: ANALYZE + VACUUM
echo "Running ANALYZE..."
sqlite3 "$DB" "ANALYZE;"
echo "Running VACUUM + journal_mode=DELETE..."
sqlite3 "$DB" "VACUUM; PRAGMA journal_mode=DELETE;"

echo "DB size after: $(du -h "$DB" | cut -f1)"
echo "Done! Cache tables created."
