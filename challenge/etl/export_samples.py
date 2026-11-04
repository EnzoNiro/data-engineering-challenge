"""Exporta amostras e resumos em CSV (outputs/samples/) para conferir os dados sem precisar de um cliente SQL."""
import logging
from contextlib import closing

from etl import config

log = logging.getLogger(__name__)

EXPORTS = {
    "daily_summary": f"SELECT * FROM {config.DAILY_SUMMARY_TABLE} ORDER BY trip_date",
    "raw_sample": f"SELECT * FROM {config.RAW_TABLE} WHERE random() < 0.00001 LIMIT 200",
    "trusted_sample": f"SELECT * FROM {config.TRUSTED_TABLE} WHERE random() < 0.00001 LIMIT 200",
    "refined_sample": f"SELECT * FROM {config.REFINED_TABLE} WHERE random() < 0.00001 LIMIT 200",
    "raw_rejected_sample": (
        f"SELECT r.* FROM {config.RAW_TABLE} r WHERE NOT (r.tpep_pickup_datetime IS NOT NULL "
        "AND r.tpep_dropoff_datetime IS NOT NULL AND r.tpep_dropoff_datetime >= r.tpep_pickup_datetime "
        f"AND r.tpep_pickup_datetime >= DATE '{config.YEAR}-01-01' "
        f"AND r.tpep_pickup_datetime < DATE '{config.YEAR + 1}-01-01' "
        "AND r.trip_distance IS NOT NULL AND r.trip_distance >= 0) LIMIT 200"
    ),
    "longest_trips_top20": (
        f"SELECT trip_date, trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, "
        f"trip_duration_min, avg_speed_mph, is_plausible FROM {config.REFINED_TABLE} "
        "ORDER BY trip_distance DESC, tpep_pickup_datetime LIMIT 20"
    ),
    "implausible_sample": f"SELECT * FROM {config.REFINED_TABLE} WHERE NOT is_plausible LIMIT 200",
}


def export_samples() -> None:
    out = config.OUTPUT_DIR / "samples"
    out.mkdir(parents=True, exist_ok=True)
    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for name, query in EXPORTS.items():
                path = out / f"{name}.csv"
                with open(path, "w", encoding="utf-8", newline="") as fh:
                    cur.copy_expert(f"COPY ({query}) TO STDOUT WITH CSV HEADER", fh)
                log.info("CSV gerado: %s", path)
