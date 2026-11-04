-- Q4. Distribuição da distância percorrida (milhas): média, desvio padrão, mínimo, máximo e quartis
SELECT
    'todas as viagens (trusted)'                                                           AS base,
    count(*)                                                                               AS viagens,
    round(avg(trip_distance)::numeric, 4)                                                  AS media,
    round(stddev_samp(trip_distance)::numeric, 4)                                          AS desvio_padrao,
    min(trip_distance)                                                                     AS minimo,
    round((percentile_cont(0.25) WITHIN GROUP (ORDER BY trip_distance))::numeric, 4)       AS q1_25,
    round((percentile_cont(0.50) WITHIN GROUP (ORDER BY trip_distance))::numeric, 4)       AS mediana_50,
    round((percentile_cont(0.75) WITHIN GROUP (ORDER BY trip_distance))::numeric, 4)       AS q3_75,
    max(trip_distance)                                                                     AS maximo
FROM refined.trips
UNION ALL
SELECT
    'somente viagens plausiveis',
    count(*),
    round(avg(trip_distance)::numeric, 4),
    round(stddev_samp(trip_distance)::numeric, 4),
    min(trip_distance),
    round((percentile_cont(0.25) WITHIN GROUP (ORDER BY trip_distance))::numeric, 4),
    round((percentile_cont(0.50) WITHIN GROUP (ORDER BY trip_distance))::numeric, 4),
    round((percentile_cont(0.75) WITHIN GROUP (ORDER BY trip_distance))::numeric, 4),
    max(trip_distance)
FROM refined.trips
WHERE is_plausible;
