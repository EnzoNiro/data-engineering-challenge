-- Q2. Total de viagens iniciadas e finalizadas no dia 17 de junho (2022)
SELECT
    count(*) FILTER (
        WHERE tpep_pickup_datetime >= DATE '2022-06-17'
          AND tpep_pickup_datetime <  DATE '2022-06-18'
    ) AS viagens_iniciadas,
    count(*) FILTER (
        WHERE tpep_dropoff_datetime >= DATE '2022-06-17'
          AND tpep_dropoff_datetime <  DATE '2022-06-18'
    ) AS viagens_finalizadas,
    count(*) FILTER (
        WHERE tpep_pickup_datetime  >= DATE '2022-06-17'
          AND tpep_pickup_datetime  <  DATE '2022-06-18'
          AND tpep_dropoff_datetime >= DATE '2022-06-17'
          AND tpep_dropoff_datetime <  DATE '2022-06-18'
    ) AS iniciadas_e_finalizadas_no_dia
FROM refined.trips
WHERE tpep_pickup_datetime  <  DATE '2022-06-18'
  AND tpep_dropoff_datetime >= DATE '2022-06-17';
