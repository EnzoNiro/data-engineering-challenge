"""Checagens de qualidade pós-carga + relatório em outputs/data_quality.md."""
import datetime as dt
import logging
from contextlib import closing

from etl import config
from etl.markdown import to_md_table

log = logging.getLogger(__name__)

RULES_SQL = f"""
SELECT
    count(*)                                                                      AS total_raw,
    count(*) FILTER (WHERE tpep_pickup_datetime IS NULL
                        OR tpep_dropoff_datetime IS NULL)                         AS timestamps_nulos,
    count(*) FILTER (WHERE tpep_dropoff_datetime < tpep_pickup_datetime)          AS dropoff_antes_do_pickup,
    count(*) FILTER (WHERE tpep_pickup_datetime <  DATE '{config.YEAR}-01-01'
                        OR tpep_pickup_datetime >= DATE '{config.YEAR + 1}-01-01') AS pickup_fora_de_{config.YEAR},
    count(*) FILTER (WHERE trip_distance IS NULL OR trip_distance < 0)            AS distancia_invalida
FROM {config.RAW_TABLE}
"""


def run_quality_checks() -> None:
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            counts = {}
            for label, table in [
                ("raw", config.RAW_TABLE),
                ("trusted", config.TRUSTED_TABLE),
                ("refined", config.REFINED_TABLE),
            ]:
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[label] = cur.fetchone()[0]
            log.info("Contagens por camada: %s", counts)

            if counts["raw"] == 0:
                raise ValueError("raw está vazia")
            if counts["trusted"] > counts["raw"]:
                raise ValueError("trusted tem mais linhas que raw")
            if counts["refined"] != counts["trusted"]:
                raise ValueError("refined deveria ter o mesmo número de linhas que trusted")

            cur.execute(RULES_SQL)
            rule_cols = [d[0] for d in cur.description]
            rule_rows = cur.fetchall()

            cur.execute(f"SELECT count(*) FILTER (WHERE is_plausible), count(*) FROM {config.REFINED_TABLE}")
            plausible, total = cur.fetchone()

            for table in (
                config.RAW_TABLE,
                config.TRUSTED_TABLE,
                config.REFINED_TABLE,
                config.DAILY_SUMMARY_TABLE,
            ):
                cur.execute(f"ANALYZE {table}")

    rejected = counts["raw"] - counts["trusted"]
    layer_table = to_md_table(
        ["camada", "registros"],
        [("raw", counts["raw"]), ("trusted", counts["trusted"]), ("refined", counts["refined"])],
    )
    report = "\n".join(
        [
            "# Relatório de qualidade dos dados",
            "",
            f"Gerado em {dt.datetime.now():%Y-%m-%d %H:%M:%S}.",
            "",
            "## Registros por camada",
            "",
            layer_table,
            "",
            f"Registros descartados na trusted: **{rejected}** "
            f"({rejected / counts['raw']:.4%} da raw).",
            "",
            "## Violações por regra (medidas sobre a raw; um registro pode violar mais de uma regra)",
            "",
            to_md_table(rule_cols, rule_rows),
            "",
            "## Plausibilidade (refined.trips)",
            "",
            f"Viagens com `is_plausible = true`: **{plausible}** de **{total}** "
            f"({plausible / total:.4%}).",
            "",
        ]
    )
    (config.OUTPUT_DIR / "data_quality.md").write_text(report, encoding="utf-8")
    log.info("Relatório gravado em %s", config.OUTPUT_DIR / "data_quality.md")
