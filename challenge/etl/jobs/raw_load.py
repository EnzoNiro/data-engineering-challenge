"""Camada RAW: lê os 12 parquets mensais (PySpark) e grava em raw.yellow_tripdata.

Nenhuma linha é descartada aqui. Cada arquivo é lido separadamente porque os tipos
variam entre meses; depois tudo é convertido para o schema canônico e unido.
"""
import logging
import re
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from etl import config
from etl.schema import CANONICAL_COLUMNS
from etl.spark_utils import get_spark, write_table

log = logging.getLogger("raw_load")
MONTH_RE = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})")


def read_month(spark: SparkSession, path: Path) -> DataFrame:
    df = spark.read.parquet(str(path))
    df = df.toDF(*[c.lower() for c in df.columns])  # airport_fee x Airport_fee etc.

    columns = []
    for name, dtype in CANONICAL_COLUMNS:
        if name in df.columns:
            columns.append(F.col(name).cast(dtype).alias(name))
        else:  # coluna ausente em algum mês => nula
            columns.append(F.lit(None).cast(dtype).alias(name))

    month = int(MONTH_RE.search(path.name).group(2))
    return (
        df.select(*columns)
        .withColumn("source_file", F.lit(path.name))
        .withColumn("file_month", F.lit(month))
        .withColumn("ingested_at", F.current_timestamp())
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    paths = sorted(config.LANDING_DIR.glob("yellow_tripdata_*.parquet"))
    if len(paths) != 12:
        raise RuntimeError(f"Esperava 12 arquivos em {config.LANDING_DIR}, achei {len(paths)}")

    spark = get_spark("wecogno_raw_load")
    try:
        frames = [read_month(spark, p) for p in paths]
        raw = reduce(DataFrame.unionByName, frames)
        log.info("Gravando %s ...", config.RAW_TABLE)
        write_table(raw, config.RAW_TABLE)
        log.info("Camada raw concluída")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
