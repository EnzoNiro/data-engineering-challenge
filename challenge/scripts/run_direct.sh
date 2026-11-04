#!/usr/bin/env bash
# Roda o pipeline inteiro sem Airflow (mesmos jobs da DAG). Usado pelo modo direct_jobs do workflow.
# Requer as variáveis WAREHOUSE_*, SOURCE_DIR, DATA_DIR, OUTPUT_DIR, SQL_DIR e PG_JDBC_JAR.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD"
python -m etl.cli landing
python -m etl.jobs.raw_load
python -m etl.jobs.trusted_load
python -m etl.jobs.refined_load
python -m etl.cli quality
python -m etl.cli answers
python -m etl.cli samples
python -m etl.cli validate
