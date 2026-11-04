"""
### WeCogno | ETL de corridas de táxi de Nova York (2022)

    prepare_landing -> load_raw -> load_trusted -> load_refined -> quality_checks -> generate_answers -> export_samples -> validate_results

* `prepare_landing`: garante os 12 arquivos mensais (baixa os que faltarem) e normaliza os nomes
* `load_raw`: PySpark lê os parquets e grava `raw.yellow_tripdata` (sem descartes)
* `load_trusted`: PySpark aplica as regras de qualidade e grava `trusted.yellow_tripdata`
* `load_refined`: PySpark enriquece e grava `refined.trips` (+ `refined.daily_summary`)
* `quality_checks`: reconcilia as contagens entre camadas e gera `outputs/data_quality.md`
* `generate_answers`: roda `sql/queries/*.sql` e gera `outputs/answers.md`
* `export_samples`: grava CSVs de amostra em `outputs/samples/`
* `validate_results`: recalcula as métricas direto dos Parquet (PyArrow/NumPy) e compara com o Postgres

Disparo manual (sem agendamento): o conjunto de dados é histórico e fixo.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl.answers import run_answers
from etl.export_samples import export_samples
from etl.landing import prepare_landing
from etl.quality import run_quality_checks
from etl.validate import run_validation

default_args = {
    "owner": "wecogno",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="wecogno_nyc_taxi_etl",
    description="ETL NYC yellow taxi 2022: raw -> trusted -> refined (PySpark + PostgreSQL)",
    start_date=datetime(2022, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["wecogno", "etl", "spark"],
    doc_md=__doc__,
) as dag:
    landing = PythonOperator(task_id="prepare_landing", python_callable=prepare_landing)

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command="cd /opt/airflow && python -m etl.jobs.raw_load",
        execution_timeout=timedelta(hours=2),
    )

    load_trusted = BashOperator(
        task_id="load_trusted",
        bash_command="cd /opt/airflow && python -m etl.jobs.trusted_load",
        execution_timeout=timedelta(hours=2),
    )

    load_refined = BashOperator(
        task_id="load_refined",
        bash_command="cd /opt/airflow && python -m etl.jobs.refined_load",
        execution_timeout=timedelta(hours=2),
    )

    quality = PythonOperator(task_id="quality_checks", python_callable=run_quality_checks)

    answers = PythonOperator(task_id="generate_answers", python_callable=run_answers)

    samples = PythonOperator(task_id="export_samples", python_callable=export_samples)

    validate = PythonOperator(task_id="validate_results", python_callable=run_validation)

    landing >> load_raw >> load_trusted >> load_refined >> quality >> answers >> samples >> validate
