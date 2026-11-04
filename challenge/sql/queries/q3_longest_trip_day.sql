-- Q3. Qual foi o dia da viagem mais longa percorrida? (maior trip_distance)
(
    SELECT
        'todas as viagens (trusted)'            AS base,
        trip_date                               AS dia_da_viagem,
        trip_distance                           AS distancia_milhas,
        tpep_pickup_datetime                    AS inicio,
        tpep_dropoff_datetime                   AS fim,
        round(trip_duration_min::numeric, 1)    AS duracao_min,
        round(avg_speed_mph::numeric, 1)        AS velocidade_media_mph
    FROM refined.trips
    ORDER BY trip_distance DESC, tpep_pickup_datetime
    LIMIT 1
)
UNION ALL
(
    SELECT
        'somente viagens plausiveis',
        trip_date,
        trip_distance,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        round(trip_duration_min::numeric, 1),
        round(avg_speed_mph::numeric, 1)
    FROM refined.trips
    WHERE is_plausible
    ORDER BY trip_distance DESC, tpep_pickup_datetime
    LIMIT 1
);
