"""Camada TRUSTED: aplica as regras de qualidade sobre a raw e grava trusted.yellow_tripdata.

Regras (registro é mantido somente se passar em TODAS):
  1. pickup e dropoff não nulos
  2. dropoff >= pickup (duração não negativa)
  3. pickup dentro do ano de referência (2022): há timestamps de 2002, 2008, 2021, 2023...
  4. trip_distance não nulo e >= 0

Não há deduplicação: a fonte não tem chave natural, e remover linhas idênticas poderia
descartar corridas legítimas.
"""
import logging

from pyspark.sql import functions as F

from etl import config
from etl.spark_utils import get_spark, read_table, write_table

log = logging.getLogger("trusted_load")


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = get_spark("wecogno_trusted_load")
    try:
        raw = read_table(spark, config.RAW_TABLE)

        pickup = F.col("tpep_pickup_datetime")
        dropoff = F.col("tpep_dropoff_datetime")
        year_start = F.lit(f"{config.YEAR}-01-01 00:00:00").cast("timestamp")
        year_end = F.lit(f"{config.YEAR + 1}-01-01 00:00:00").cast("timestamp")

        is_valid = (
            pickup.isNotNull()
            & dropoff.isNotNull()
            & (dropoff >= pickup)
            & (pickup >= year_start)
            & (pickup < year_end)
            & F.col("trip_distance").isNotNull()
            & (F.col("trip_distance") >= 0)
        )

        trusted = (
            raw.filter(is_valid)
            .withColumn("store_and_fwd_flag", F.upper(F.trim(F.col("store_and_fwd_flag"))))
            .drop("ingested_at")
            .withColumn("trusted_at", F.current_timestamp())
        )

        log.info("Gravando %s ...", config.TRUSTED_TABLE)
        write_table(trusted, config.TRUSTED_TABLE)
        log.info("Camada trusted concluída")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
