"""Camada REFINED: enriquece a trusted (PySpark) e grava refined.trips (tabela final).

Colunas derivadas:
  trip_date / dropoff_date  datas de início e fim
  trip_duration_min         duração em minutos
  avg_speed_mph             velocidade média (milhas por hora)
  is_plausible              duração entre 0 e 24h e velocidade média <= 100 mph

Nenhuma linha da trusted é removida aqui: is_plausible apenas SINALIZA outliers
(ex.: viagens de centenas de milhares de milhas em poucos minutos), para que as análises
possam ser feitas com e sem eles. Depois, o resumo diário é montado em SQL (ELT).
"""
import logging
from contextlib import closing

from pyspark.sql import functions as F

from etl import config
from etl.spark_utils import get_spark, read_table, write_table

log = logging.getLogger("refined_load")


def build_daily_summary() -> None:
    sql = (config.SQL_DIR / "transform" / "refined_daily_summary.sql").read_text(encoding="utf-8")
    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
    log.info("%s atualizada", config.DAILY_SUMMARY_TABLE)


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    spark = get_spark("wecogno_refined_load")
    try:
        trusted = read_table(spark, config.TRUSTED_TABLE)

        duration_min = (
            F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
        ) / F.lit(60.0)
        avg_speed = F.when(duration_min > 0, F.col("trip_distance") / (duration_min / F.lit(60.0)))

        trips = (
            trusted.drop("trusted_at")
            .withColumn("trip_date", F.to_date("tpep_pickup_datetime"))
            .withColumn("dropoff_date", F.to_date("tpep_dropoff_datetime"))
            .withColumn("trip_duration_min", duration_min)
            .withColumn("avg_speed_mph", avg_speed)
            .withColumn(
                "is_plausible",
                F.coalesce(
                    (duration_min > 0) & (duration_min <= 1440) & (avg_speed <= 100),
                    F.lit(False),
                ),
            )
            .withColumn("refined_at", F.current_timestamp())
        )

        log.info("Gravando %s ...", config.REFINED_TABLE)
        write_table(trips, config.REFINED_TABLE)
    finally:
        spark.stop()

    build_daily_summary()
    log.info("Camada refined concluída")


if __name__ == "__main__":
    main()
