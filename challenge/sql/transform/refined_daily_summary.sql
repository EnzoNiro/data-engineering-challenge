-- Resumo diário calculado dentro do Postgres a partir da tabela final (ELT).
TRUNCATE refined.daily_summary;

INSERT INTO refined.daily_summary
SELECT
    trip_date,
    count(*)                                          AS trips_started,
    count(*) FILTER (WHERE dropoff_date = trip_date)  AS trips_started_and_finished_same_day,
    sum(trip_distance)                                AS total_distance_miles,
    avg(trip_distance)                                AS avg_distance_miles,
    sum(total_amount)                                 AS total_amount
FROM refined.trips
GROUP BY trip_date
ORDER BY trip_date;
