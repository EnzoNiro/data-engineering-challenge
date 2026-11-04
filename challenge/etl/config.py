"""Configuração central do ETL. Tudo vem de variáveis de ambiente (ver docker-compose.yml)."""
import os
from pathlib import Path

# --- Fonte de dados ---------------------------------------------------------
YEAR = int(os.getenv("TLC_YEAR", "2022"))
SOURCE_BASE_URL = os.getenv(
    "TLC_SOURCE_BASE_URL",
    "https://github.com/datarisk-io/data-engineering-challenge/raw/master/nyc-tlc-data",
)

# --- Diretórios -------------------------------------------------------------
DATA_DIR = Path(os.getenv("DATA_DIR", "/opt/airflow/data"))
# Arquivos originais. No fork, ficam em nyc-tlc-data/ (montado em /opt/airflow/nyc-tlc-data).
SOURCE_DIR = Path(os.getenv("SOURCE_DIR", str(DATA_DIR / "source")))
LANDING_DIR = DATA_DIR / "landing"  # nomes normalizados: yellow_tripdata_YYYY-MM.parquet
OUTPUT_DIR = Path(os.getenv("OUTPUT_DIR", "/opt/airflow/outputs"))
SQL_DIR = Path(os.getenv("SQL_DIR", "/opt/airflow/sql"))

# --- Data warehouse (PostgreSQL) -------------------------------------------
WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = int(os.getenv("WAREHOUSE_PORT", "5432"))
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "wecogno_dw")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER", "wecogno")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD", "wecogno")

# reWriteBatchedInserts acelera muito os INSERTs em lote feitos pelo Spark via JDBC
JDBC_URL = (
    f"jdbc:postgresql://{WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB}"
    "?reWriteBatchedInserts=true"
)
JDBC_DRIVER = "org.postgresql.Driver"

# --- Tabelas ----------------------------------------------------------------
RAW_TABLE = "raw.yellow_tripdata"
TRUSTED_TABLE = "trusted.yellow_tripdata"
REFINED_TABLE = "refined.trips"
DAILY_SUMMARY_TABLE = "refined.daily_summary"

# --- Spark (modo local) -----------------------------------------------------
SPARK_DRIVER_MEMORY = os.getenv("SPARK_DRIVER_MEMORY", "3g")
SPARK_CORES = int(os.getenv("SPARK_LOCAL_CORES", "4"))
PG_JDBC_JAR = os.getenv("PG_JDBC_JAR", "/opt/spark-jars/postgresql-42.7.4.jar")


def get_connection():
    """Conexão psycopg2 com o warehouse (usada para SQL puro: checagens, ELT, respostas)."""
    import psycopg2

    return psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
    )
