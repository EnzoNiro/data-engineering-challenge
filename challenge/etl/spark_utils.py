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


def write_table(df: DataFrame, table: str, num_partitions: int | None = None) -> None:
    """Sobrescreve o conteúdo da tabela mantendo o DDL (truncate) => carga idempotente."""
    (
        df.write.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("user", config.WAREHOUSE_USER)
        .option("password", config.WAREHOUSE_PASSWORD)
        .option("driver", config.JDBC_DRIVER)
        .option("dbtable", table)
        .option("truncate", "true")
        .option("batchsize", "50000")
        .option("numPartitions", num_partitions or config.SPARK_CORES)
        .mode("overwrite")
        .save()
    )
