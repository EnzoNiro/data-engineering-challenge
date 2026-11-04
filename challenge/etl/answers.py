"""Executa as queries de sql/queries/ e grava as tabelas de resultado em outputs/answers.md."""
import datetime as dt
import logging
from contextlib import closing

from etl import config
from etl.markdown import to_md_table

log = logging.getLogger(__name__)


def _title(sql: str, fallback: str) -> str:
    first = sql.strip().splitlines()[0]
    return first.lstrip("- ").strip() if first.startswith("--") else fallback


def run_answers() -> None:
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    files = sorted((config.SQL_DIR / "queries").glob("q*.sql"))
    if not files:
        raise FileNotFoundError(f"Nenhuma query encontrada em {config.SQL_DIR / 'queries'}")

    parts = [
        "# Respostas do desafio",
        "",
        f"Gerado automaticamente em {dt.datetime.now():%Y-%m-%d %H:%M:%S} pela DAG "
        "`wecogno_nyc_taxi_etl`.",
        "",
    ]

    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for path in files:
                sql = path.read_text(encoding="utf-8")
                cur.execute(sql)
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
                table = to_md_table(columns, rows)
                log.info("%s\n%s", path.name, table)
                parts += [f"## {_title(sql, path.stem)}", "", "```sql", sql.strip(), "```", "", table, ""]

    (config.OUTPUT_DIR / "answers.md").write_text("\n".join(parts), encoding="utf-8")
    log.info("Respostas gravadas em %s", config.OUTPUT_DIR / "answers.md")
