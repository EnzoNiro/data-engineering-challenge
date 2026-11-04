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


def truncate_table(table: str) -> None:
    """TRUNCATE da tabela (e de todas as particoes dela). Feito em SQL de proposito: com
    mode('overwrite') + truncate o Spark emite 'TRUNCATE TABLE ONLY', que o Postgres recusa em
    tabela particionada."""
    from contextlib import closing

    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")


def write_table(df: DataFrame, table: str, num_partitions: int | None = None) -> None:
    """Recarrega a tabela: TRUNCATE (mantem o DDL e as particoes) + append => carga idempotente."""
    truncate_table(table)
    (
        df.write.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("user", config.WAREHOUSE_USER)
        .option("password", config.WAREHOUSE_PASSWORD)
        .option("driver", config.JDBC_DRIVER)
        .option("dbtable", table)
        .option("batchsize", "50000")
        .option("numPartitions", num_partitions or config.SPARK_CORES)
        .mode("append")
        .save()
    )
