"""Teste do núcleo da validação (NumPy) com dados sintéticos, sem Parquet nem Postgres.

Compara o Accumulator com um cálculo "força bruta" em Python puro, linha a linha, usando as mesmas
regras do pipeline. Execução (a partir de challenge/):

    PYTHONPATH=. python tests/test_validate_core.py
"""
import datetime as dt
import math
import random
import statistics

import numpy as np

from etl.validate import US, Accumulator

YEAR = 2022
DAY = dt.date(2022, 6, 17)


def to_us(d: dt.datetime) -> int:
    return int((d - dt.datetime(1970, 1, 1)).total_seconds()) * US


def make_rows(n: int, seed: int):
    rnd = random.Random(seed)
    rows = []
    for _ in range(n):
        pick = dt.datetime(2021, 12, 25) + dt.timedelta(seconds=rnd.randint(0, 400 * 86400))
        kind = rnd.random()
        if kind < 0.03:
            drop = pick - dt.timedelta(seconds=rnd.randint(1, 5000))  # fim antes do início
        elif kind < 0.06:
            drop = pick                                                # duração zero
        else:
            drop = pick + dt.timedelta(seconds=rnd.randint(30, 4 * 3600))
        if rnd.random() < 0.01:
            drop = pick + dt.timedelta(seconds=rnd.randint(25 * 3600, 40 * 3600))  # > 24 h
        dist = round(rnd.expovariate(1 / 3.0), 2)
        r = rnd.random()
        if r < 0.02:
            dist = -dist                                               # distância negativa
        elif r < 0.03:
            dist = rnd.choice([389678.46, 5000.0, 150.0])              # outlier
        elif r < 0.04:
            dist = 0.0
        null_pick = rnd.random() < 0.01
        null_drop = rnd.random() < 0.01
        null_dist = rnd.random() < 0.01
        rows.append((pick, drop, dist, null_pick, null_drop, null_dist))
    return rows


def brute_force(rows):
    y0, y1 = dt.datetime(YEAR, 1, 1), dt.datetime(YEAR + 1, 1, 1)
    d0, d1 = dt.datetime(2022, 6, 17), dt.datetime(2022, 6, 18)
    valid, plausible = [], []
    raw_part = [0] * 12
    valid_part = [0] * 12
    started = finished = both = 0
    for pick, drop, dist, np_, nd_, ndist in rows:
        if not np_ and y0 <= pick < y1:
            raw_part[pick.month - 1] += 1
        if np_ or nd_ or ndist:
            continue
        if not (drop >= pick and y0 <= pick < y1 and dist >= 0):
            continue
        dur = ((to_us(drop) // US) - (to_us(pick) // US)) / 60.0
        valid.append((dist, pick))
        valid_part[pick.month - 1] += 1
        speed = dist / (dur / 60.0) if dur > 0 else math.nan
        if dur > 0 and dur <= 1440 and speed <= 100:
            plausible.append((dist, pick))
        ip, idr = d0 <= pick < d1, d0 <= drop < d1
        started += ip
        finished += idr
        both += ip and idr
    return valid, plausible, (started, finished, both), raw_part, valid_part


def stats(pairs):
    values = [d for d, _ in pairs]
    q = np.percentile(np.array(values), [25, 50, 75])
    best = max(values)
    best_pick = min(p for d, p in pairs if d == best)
    return {
        "n": len(values), "mean": statistics.fmean(values), "std": statistics.stdev(values),
        "min": min(values), "q1": q[0], "median": q[1], "q3": q[2], "max": best,
        "best_date": best_pick.date(),
    }


def check(rows, chunk):
    acc = Accumulator(YEAR, DAY)
    for i in range(0, len(rows), chunk):
        part = rows[i:i + chunk]
        acc.add(
            np.array([to_us(r[0]) for r in part], dtype="int64"), np.array([not r[3] for r in part]),
            np.array([to_us(r[1]) for r in part], dtype="int64"), np.array([not r[4] for r in part]),
            np.array([r[2] for r in part], dtype="float64"), np.array([not r[5] for r in part]),
        )
    got = acc.expected()
    valid, plausible, q2, raw_part, valid_part = brute_force(rows)

    assert got["rows"] == len(rows)
    assert got["valid"] == len(valid), (got["valid"], len(valid))
    assert got["raw_part"][:12] == raw_part, (got["raw_part"], raw_part)
    assert got["raw_part"][12] == len(rows) - sum(raw_part)  # DEFAULT: nulo ou fora do ano
    assert got["valid_part"] == valid_part, (got["valid_part"], valid_part)
    assert sum(got["valid_part"]) == got["valid"]
    assert (got["q2"]["started"], got["q2"]["finished"], got["q2"]["both"]) == q2, (got["q2"], q2)
    for key, pairs in (("all", valid), ("plausible", plausible)):
        want, have = stats(pairs), got[key]
        assert have["n"] == want["n"], (key, have["n"], want["n"])
        assert have["best_date"] == want["best_date"], (key, have["best_date"], want["best_date"])
        for stat in ("mean", "std", "min", "q1", "median", "q3", "max"):
            assert math.isclose(have[stat], want[stat], rel_tol=1e-9, abs_tol=1e-9), (key, stat, have[stat], want[stat])
        assert have["best_distance"] == want["max"]
    return got


if __name__ == "__main__":
    for seed, chunk in ((1, 500), (2, 777), (3, 100000)):
        rows = make_rows(20000, seed)
        got = check(rows, chunk)
        print(f"seed={seed} chunk={chunk}: OK | válidas={got['valid']} plausíveis={got['plausible']['n']} "
              f"Q2={tuple(got['q2'].values())}")
    print("Todos os testes do núcleo passaram.")
