"""Helpers de SparkSession e de leitura/escrita JDBC no PostgreSQL."""
from pyspark.sql import DataFrame, SparkSession

from etl import config


def get_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{config.SPARK_CORES}]")
        .config("spark.driver.memory", config.SPARK_DRIVER_MEMORY)
        .config("spark.jars", config.PG_JDBC_JAR)
        # Fuso fixo em UTC: o horário das corridas é "relógio de parede" e não pode ser deslocado
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.parquet.inferTimestampNTZ.enabled", "false")
        .config("spark.sql.shuffle.partitions", "24")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _jdbc_reader(spark: SparkSession):
    return (
        spark.read.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("user", config.WAREHOUSE_USER)
        .option("password", config.WAREHOUSE_PASSWORD)
        .option("driver", config.JDBC_DRIVER)
    )


def read_table(
    spark: SparkSession,
    table: str,
    partition_column: str = "file_month",
    lower: int = 1,
    upper: int = 12,
    partitions: int = 12,
) -> DataFrame:
    """Leitura paralela: uma partição Spark por mês de origem (coluna file_month)."""
    return (
        _jdbc_reader(spark)
        .option("dbtable", table)
        .option("partitionColumn", partition_column)
        .option("lowerBound", lower)
        .option("upperBound", upper)
        .option("numPartitions", partitions)
        .option("fetchsize", "20000")
        .load()
    )


def _swap_into(table: str, staging: str) -> None:
    """Troca atômica: numa única transação esvazia a tabela final (e as partições) e copia a staging.
    Se algo falhar, o ROLLBACK deixa a tabela final exatamente como estava."""
    from contextlib import closing

    with closing(config.get_connection()) as conn:
        with conn:  # commit no fim do bloco; rollback se houver exceção
            with conn.cursor() as cur:
                cur.execute(f"TRUNCATE TABLE {table}")
                cur.execute(f"INSERT INTO {table} SELECT * FROM {staging}")


def _execute(sql: str) -> None:
    from contextlib import closing

    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)


def write_table(df: DataFrame, table: str, num_partitions: int | None = None) -> None:
    """Recarrega a tabela sem deixá-la vazia ou pela metade se a carga falhar:

    1. o Spark grava em <tabela>_staging (cópia da estrutura, sem partições);
    2. só depois do sucesso, uma transação faz TRUNCATE da tabela final + INSERT ... SELECT da staging;
    3. a staging é apagada.

    Falha no passo 1 não toca na tabela final. Rodar de novo recomeça da staging limpa (idempotente).
    """
    staging = f"{table}_staging"
    _execute(f"DROP TABLE IF EXISTS {staging}")
    _execute(f"CREATE UNLOGGED TABLE {staging} (LIKE {table} INCLUDING DEFAULTS)")
    (
        df.write.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("user", config.WAREHOUSE_USER)
        .option("password", config.WAREHOUSE_PASSWORD)
        .option("driver", config.JDBC_DRIVER)
        .option("dbtable", staging)
        .option("batchsize", "50000")
        .option("numPartitions", num_partitions or config.SPARK_CORES)
        .mode("append")
        .save()
    )
    _swap_into(table, staging)
    _execute(f"DROP TABLE {staging}")
