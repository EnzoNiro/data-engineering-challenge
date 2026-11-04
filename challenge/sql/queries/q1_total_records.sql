-- Q1. Qual o total de registros na tabela final? (tabela final = refined.trips)
SELECT 'raw.yellow_tripdata'     AS tabela, count(*) AS registros FROM raw.yellow_tripdata
UNION ALL
SELECT 'trusted.yellow_tripdata' AS tabela, count(*) AS registros FROM trusted.yellow_tripdata
UNION ALL
SELECT 'refined.trips (final)'   AS tabela, count(*) AS registros FROM refined.trips;
