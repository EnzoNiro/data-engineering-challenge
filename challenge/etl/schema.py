"""Schema canônico da camada raw.

Os arquivos mensais do TLC NÃO têm tipos consistentes entre si (ex.: passenger_count e
RatecodeID ora são inteiros, ora double; a coluna de taxa de aeroporto muda de caixa).
Por isso cada arquivo é lido separadamente e convertido para este schema antes do union.
"""
from pyspark.sql import types as T

MONEY = T.DecimalType(12, 2)

CANONICAL_COLUMNS = [
    ("vendorid", T.IntegerType()),
    ("tpep_pickup_datetime", T.TimestampType()),
    ("tpep_dropoff_datetime", T.TimestampType()),
    ("passenger_count", T.IntegerType()),
    ("trip_distance", T.DoubleType()),
    ("ratecodeid", T.IntegerType()),
    ("store_and_fwd_flag", T.StringType()),
    ("pulocationid", T.IntegerType()),
    ("dolocationid", T.IntegerType()),
    ("payment_type", T.IntegerType()),
    ("fare_amount", MONEY),
    ("extra", MONEY),
    ("mta_tax", MONEY),
    ("tip_amount", MONEY),
    ("tolls_amount", MONEY),
    ("improvement_surcharge", MONEY),
    ("total_amount", MONEY),
    ("congestion_surcharge", MONEY),
    ("airport_fee", MONEY),
]
