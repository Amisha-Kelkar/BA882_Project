#!/bin/bash
set -euo pipefail
: "${MOTHERDUCK_TOKEN:?Set MOTHERDUCK_TOKEN first (export MOTHERDUCK_TOKEN=...)}"

WAREHOUSE_FILE="${1:-$HOME/Yucheng/target_warehouse.duckdb}"
DB_SCHEMA="project_data.raw_data"
TABLE="store_data"

duckdb "md:?token=${MOTHERDUCK_TOKEN}" <<SQL
ATTACH '${WAREHOUSE_FILE}' AS local (READ_ONLY);
CREATE DATABASE IF NOT EXISTS project_data;
CREATE SCHEMA IF NOT EXISTS ${DB_SCHEMA};
CREATE OR REPLACE TABLE ${DB_SCHEMA}.${TABLE} AS
SELECT * FROM local.target_stores;
SELECT COUNT(*) AS rows FROM ${DB_SCHEMA}.${TABLE};
SQL
