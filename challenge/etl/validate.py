"""Validação independente do pipeline.

Recalcula, direto dos 12 arquivos Parquet (PyArrow + NumPy, SEM Spark e SEM Postgres), as mesmas
métricas que o pipeline gravou no Postgres e compara os dois lados:

  * linhas por mês e total da raw (rodapé dos Parquet x raw.yellow_tripdata)
  * linhas da trusted e da refined (regras de qualidade reaplicadas em NumPy)
  * viagens plausíveis (mesma fórmula de duração e velocidade da refined)
  * Q2 (17 de junho), Q3 (viagem mais longa) e Q4 (média, desvio, mín, máx e quartis)

Se alguma checagem falhar, o relatório outputs/validation.md mostra qual e a tarefa falha.

Também oferece o teste de idempotência: `snapshot_counts` guarda as contagens por tabela e
`compare_counts` confere se, depois de rodar o pipeline de novo, nada mudou (nada duplicou).
"""
import datetime as dt
import json
import logging
import math
from contextlib import closing
from pathlib import Path

from etl import config
from etl.markdown import to_md_table

log = logging.getLogger(__name__)

US = 1_000_000  # microssegundos por segundo
Q2_DAY = (6, 17)  # 17 de junho


def _us(timestamp: str) -> int:
    import numpy as np

    return int(np.datetime64(timestamp, "us").astype("int64"))


def _us_to_date(us: int) -> dt.date:
    return (dt.datetime(1970, 1, 1) + dt.timedelta(microseconds=int(us))).date()


class Accumulator:
    """Acumula, arquivo a arquivo, o necessário para calcular as respostas esperadas.

    Recebe arrays NumPy (timestamps em microssegundos, distância em double) e máscaras de
    validade (False = nulo no Parquet). As regras e fórmulas espelham as do pipeline:
    trusted_load.py (regras de qualidade) e refined_load.py (is_plausible).
    """

    def __init__(self, year: int, day: dt.date):
        import numpy as np

        self.np = np
        self.y0 = _us(f"{year}-01-01T00:00:00")
        self.y1 = _us(f"{year + 1}-01-01T00:00:00")
        self.d0 = _us(f"{day.isoformat()}T00:00:00")
        self.d1 = self.d0 + 86_400 * US
        self.rows = 0
        self.valid = 0
        self.started = self.finished = self.both = 0
        self._all: list = []
        self._pl: list = []
        # fronteiras mensais (13 valores) para contar linhas por partição esperada
        self.bounds = np.array(
            [_us(f"{year}-{m:02d}-01T00:00:00") for m in range(1, 13)] + [self.y1], dtype="int64"
        )
        self.raw_part = np.zeros(12, dtype="int64")    # raw: linhas com pickup em cada mês do ano
        self.valid_part = np.zeros(12, dtype="int64")  # trusted: linhas válidas por mês
        self.best_all = None  # (distancia, pickup_us)
        self.best_pl = None

    def _month(self, pick):
        """Índice do mês (0 a 11) de cada pickup; só chamar com pickups já dentro do ano."""
        return self.np.searchsorted(self.bounds, pick, side="right") - 1

    @staticmethod
    def _best(best, x, p):
        if len(x) == 0:
            return best
        top = x.max()
        candidate = (float(top), int(p[x == top].min()))  # desempate: pickup mais antigo
        if best is None or candidate[0] > best[0] or (candidate[0] == best[0] and candidate[1] < best[1]):
            return candidate
        return best

    def add(self, pick, pick_ok, drop, drop_ok, dist, dist_ok) -> None:
        np = self.np
        self.rows += len(pick)
        in_year = pick_ok & (pick >= self.y0) & (pick < self.y1)
        self.raw_part += np.bincount(self._month(pick[in_year]), minlength=12)
        with np.errstate(all="ignore"):
            valid = (
                pick_ok & drop_ok & dist_ok
                & (drop >= pick)
                & (pick >= self.y0) & (pick < self.y1)
                & (dist >= 0)
            )
            p, d, x = pick[valid], drop[valid], dist[valid]
            self.valid += int(len(x))
            self.valid_part += np.bincount(self._month(p), minlength=12)
            if len(x) == 0:
                return

            dur_min = ((d // US) - (p // US)) / 60.0
            speed = np.where(dur_min > 0, x / (dur_min / 60.0), np.nan)
            plausible = (dur_min > 0) & (dur_min <= 1440) & (speed <= 100)

            in_p = (p >= self.d0) & (p < self.d1)
            in_d = (d >= self.d0) & (d < self.d1)
            self.started += int(in_p.sum())
            self.finished += int(in_d.sum())
            self.both += int((in_p & in_d).sum())

            self._all.append(x)
            self._pl.append(x[plausible])
            self.best_all = self._best(self.best_all, x, p)
            self.best_pl = self._best(self.best_pl, x[plausible], p[plausible])

    def expected(self) -> dict:
        np = self.np
        out = {
            "rows": self.rows,
            "valid": self.valid,
            "raw_part": [int(v) for v in self.raw_part] + [self.rows - int(self.raw_part.sum())],
            "valid_part": [int(v) for v in self.valid_part],
            "q2": {"started": self.started, "finished": self.finished, "both": self.both},
        }
        for key, chunks, best in (("all", self._all, self.best_all), ("plausible", self._pl, self.best_pl)):
            values = np.concatenate(chunks) if chunks else np.array([], dtype="float64")
            if len(values) == 0:
                out[key] = None
                continue
            q1, median, q3 = np.percentile(values, [25, 50, 75])  # interpolação linear
            out[key] = {
                "n": int(len(values)),
                "mean": float(values.mean()),
                "std": float(values.std(ddof=1)) if len(values) > 1 else None,
                "min": float(values.min()),
                "q1": float(q1),
                "median": float(median),
                "q3": float(q3),
                "max": float(values.max()),
                "best_distance": best[0],
                "best_date": _us_to_date(best[1]),
            }
        return out


# --------------------------------------------------------------------------------------
# Lado Parquet
# --------------------------------------------------------------------------------------
def read_columns(path: Path):
    """Lê só as 3 colunas necessárias de um Parquet e devolve arrays NumPy + máscaras."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    names = {n.lower(): n for n in pq.ParquetFile(str(path)).schema_arrow.names}
    wanted = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance"]
    table = pq.read_table(str(path), columns=[names[w] for w in wanted])

    def timestamp(column):
        arr = pc.cast(table.column(names[column]).combine_chunks(), pa.timestamp("us"))
        ok = pc.is_valid(arr).to_numpy(zero_copy_only=False)
        values = pc.fill_null(arr.cast(pa.int64()), 0).to_numpy(zero_copy_only=False)
        return values, ok

    pick, pick_ok = timestamp("tpep_pickup_datetime")
    drop, drop_ok = timestamp("tpep_dropoff_datetime")
    arr = pc.cast(table.column(names["trip_distance"]).combine_chunks(), pa.float64())
    dist_ok = pc.is_valid(arr).to_numpy(zero_copy_only=False)
    dist = pc.fill_null(arr, 0.0).to_numpy(zero_copy_only=False)
    return pick, pick_ok, drop, drop_ok, dist, dist_ok


def parquet_expected(files: list[Path], year: int) -> tuple[dict, dict]:
    import pyarrow.parquet as pq

    acc = Accumulator(year, dt.date(year, *Q2_DAY))
    footer_rows = {}
    for path in files:
        month = int(path.stem.split("-")[-1])
        footer_rows[month] = pq.ParquetFile(str(path)).metadata.num_rows
        log.info("Lendo %s (%s linhas)", path.name, footer_rows[month])
        acc.add(*read_columns(path))
    return acc.expected(), footer_rows


# --------------------------------------------------------------------------------------
# Lado Postgres
# --------------------------------------------------------------------------------------
def _stats_sql(table: str, where: str) -> str:
    q = "percentile_cont({p}) WITHIN GROUP (ORDER BY trip_distance)"
    return (
        "SELECT count(*), avg(trip_distance), stddev_samp(trip_distance), min(trip_distance), "
        f"{q.format(p=0.25)}, {q.format(p=0.5)}, {q.format(p=0.75)}, max(trip_distance) "
        f"FROM {table} {where}"
    )


def partition_counts(cur, table: str) -> list[int]:
    """Linhas por partição física: 12 meses (índice 0 a 11) e a DEFAULT (índice 12)."""
    cur.execute(f"SELECT c.relname, count(*) FROM {table} t JOIN pg_class c ON c.oid = t.tableoid GROUP BY 1")
    counts = [0] * 13
    for relname, n in cur.fetchall():
        counts[12 if relname.endswith("_default") else int(relname[-2:]) - 1] = int(n)
    return counts


def postgres_observed(cur, year: int) -> dict:
    day = dt.date(year, *Q2_DAY)
    nxt = day + dt.timedelta(days=1)
    obs: dict = {}

    cur.execute(f"SELECT file_month, count(*) FROM {config.RAW_TABLE} GROUP BY 1")
    obs["raw_by_month"] = {int(m): int(n) for m, n in cur.fetchall()}
    for label, table in (("trusted", config.TRUSTED_TABLE), ("refined", config.REFINED_TABLE)):
        cur.execute(f"SELECT count(*) FROM {table}")
        obs[label] = int(cur.fetchone()[0])

    for label, table in (("raw", config.RAW_TABLE), ("trusted", config.TRUSTED_TABLE), ("refined", config.REFINED_TABLE)):
        obs[f"part_{label}"] = partition_counts(cur, table)

    cur.execute(
        "SELECT "
        f"count(*) FILTER (WHERE tpep_pickup_datetime >= DATE '{day}' AND tpep_pickup_datetime < DATE '{nxt}'), "
        f"count(*) FILTER (WHERE tpep_dropoff_datetime >= DATE '{day}' AND tpep_dropoff_datetime < DATE '{nxt}'), "
        f"count(*) FILTER (WHERE tpep_pickup_datetime >= DATE '{day}' AND tpep_pickup_datetime < DATE '{nxt}' "
        f"AND tpep_dropoff_datetime >= DATE '{day}' AND tpep_dropoff_datetime < DATE '{nxt}') "
        f"FROM {config.REFINED_TABLE}"
    )
    started, finished, both = cur.fetchone()
    obs["q2"] = {"started": int(started), "finished": int(finished), "both": int(both)}

    for key, where in (("all", ""), ("plausible", "WHERE is_plausible")):
        cur.execute(_stats_sql(config.REFINED_TABLE, where))
        n, mean, std, mn, q1, median, q3, mx = cur.fetchone()
        cur.execute(
            f"SELECT trip_distance, trip_date FROM {config.REFINED_TABLE} {where} "
            "ORDER BY trip_distance DESC, tpep_pickup_datetime LIMIT 1"
        )
        best = cur.fetchone()
        obs[key] = {
            "n": int(n), "mean": mean, "std": std, "min": mn,
            "q1": q1, "median": median, "q3": q3, "max": mx,
            "best_distance": best[0] if best else None,
            "best_date": best[1] if best else None,
        }
    return obs


# --------------------------------------------------------------------------------------
# Comparação e relatório
# --------------------------------------------------------------------------------------
def _close(a, b) -> bool:
    if a is None or b is None:
        return a is b
    return math.isclose(float(a), float(b), rel_tol=1e-6, abs_tol=1e-6)


def build_checks(expected: dict, footer_rows: dict, observed: dict) -> list[tuple]:
    checks: list[tuple] = []

    def add(name, exp, obs, close=False):
        ok = _close(exp, obs) if close else exp == obs
        checks.append((name, exp, obs, "OK" if ok else "FALHOU"))

    for month in sorted(footer_rows):
        add(f"raw: linhas do mês {month:02d}", footer_rows[month], observed["raw_by_month"].get(month, 0))
    add("raw: total de linhas", sum(footer_rows.values()), sum(observed["raw_by_month"].values()))
    add("trusted: linhas (regras reaplicadas)", expected["valid"], observed["trusted"])
    add("refined: linhas (= trusted)", expected["valid"], observed["refined"])

    names = [f"{m:02d}" for m in range(1, 13)] + ["DEFAULT"]
    exp_raw = expected["raw_part"]
    exp_trusted = expected["valid_part"] + [0]
    for i, name in enumerate(names):
        add(f"partição raw {name}", exp_raw[i], observed["part_raw"][i])
    for i, name in enumerate(names):
        add(f"partição trusted {name}", exp_trusted[i], observed["part_trusted"][i])
    add("partições refined = partições trusted", exp_trusted, observed["part_refined"])

    for key, label in (("started", "iniciadas"), ("finished", "finalizadas"), ("both", "iniciadas e finalizadas")):
        add(f"Q2: viagens {label} em 17/06", expected["q2"][key], observed["q2"][key])

    for key, label in (("all", "todas as viagens"), ("plausible", "plausíveis")):
        exp, obs = expected[key], observed[key]
        if exp is None or obs["n"] == 0:
            add(f"{label}: existem viagens", exp is not None, obs["n"] > 0)
            continue
        add(f"{label}: quantidade", exp["n"], obs["n"])
        add(f"Q3 ({label}): distância da viagem mais longa", exp["best_distance"], obs["best_distance"], close=True)
        add(f"Q3 ({label}): dia da viagem mais longa", exp["best_date"], obs["best_date"])
        for stat, name in (("mean", "média"), ("std", "desvio padrão"), ("min", "mínimo"), ("q1", "Q1"),
                           ("median", "mediana"), ("q3", "Q3"), ("max", "máximo")):
            add(f"Q4 ({label}): {name}", exp[stat], obs[stat], close=True)
    return checks


def run_validation() -> None:
    year = config.YEAR
    files = sorted(config.LANDING_DIR.glob("yellow_tripdata_*.parquet"))
    if len(files) != 12:
        raise RuntimeError(f"Esperava 12 arquivos em {config.LANDING_DIR}, achei {len(files)}")

    expected, footer_rows = parquet_expected(files, year)
    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            observed = postgres_observed(cur, year)

    checks = build_checks(expected, footer_rows, observed)
    failed = [c for c in checks if c[3] != "OK"]

    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = "\n".join([
        "# Validação independente",
        "",
        f"Gerado em {dt.datetime.now():%Y-%m-%d %H:%M:%S}.",
        "",
        "Os valores **esperados** foram recalculados direto dos 12 arquivos Parquet com PyArrow e "
        "NumPy (sem Spark e sem Postgres). Os **obtidos** vêm do Postgres, preenchido pelo pipeline.",
        "",
        f"**Resultado: {len(checks) - len(failed)} de {len(checks)} checagens OK.**",
        "",
        to_md_table(["checagem", "esperado (Parquet)", "obtido (Postgres)", "status"], checks),
        "",
    ])
    (config.OUTPUT_DIR / "validation.md").write_text(report, encoding="utf-8")
    log.info("Validação: %d de %d checagens OK", len(checks) - len(failed), len(checks))
    for name, exp, obs, _ in failed:
        log.error("FALHOU: %s | esperado=%s obtido=%s", name, exp, obs)
    if failed:
        raise AssertionError(f"{len(failed)} checagem(ns) falharam; veja outputs/validation.md")


# --------------------------------------------------------------------------------------
# Idempotência
# --------------------------------------------------------------------------------------
def _table_counts() -> dict:
    tables = {
        "raw": config.RAW_TABLE,
        "trusted": config.TRUSTED_TABLE,
        "refined": config.REFINED_TABLE,
        "daily_summary": config.DAILY_SUMMARY_TABLE,
    }
    with closing(config.get_connection()) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            counts = {}
            for label, table in tables.items():
                cur.execute(f"SELECT count(*) FROM {table}")
                counts[label] = int(cur.fetchone()[0])
    return counts


def snapshot_counts(path: str) -> dict:
    counts = _table_counts()
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(counts, indent=2), encoding="utf-8")
    log.info("Contagens gravadas em %s: %s", path, counts)
    return counts


def compare_counts(path: str) -> None:
    before = json.loads(Path(path).read_text(encoding="utf-8"))
    after = _table_counts()
    rows = [(t, before[t], after[t], "OK" if before[t] == after[t] else "FALHOU") for t in before]
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    report = "\n".join([
        "# Teste de idempotência",
        "",
        "O pipeline foi executado duas vezes. Se as cargas fossem append, as contagens da 2ª "
        "execução seriam maiores. Contagens iguais significam que nada foi duplicado.",
        "",
        to_md_table(["tabela", "após a 1ª execução", "após a 2ª execução", "status"], rows),
        "",
    ])
    (config.OUTPUT_DIR / "idempotency.md").write_text(report, encoding="utf-8")
    log.info("Idempotência:\n%s", to_md_table(["tabela", "1ª", "2ª", "status"], rows))
    if any(r[3] != "OK" for r in rows):
        raise AssertionError("As contagens mudaram entre as execuções; veja outputs/idempotency.md")
