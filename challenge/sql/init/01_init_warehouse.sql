-- Executado automaticamente pelo Postgres na primeira criação do volume do warehouse.
-- Para recriar do zero: docker compose down -v

CREATE SCHEMA IF NOT EXISTS raw;       -- dado como chegou da fonte (tipos padronizados + metadados)
CREATE SCHEMA IF NOT EXISTS trusted;   -- dado limpo e validado
CREATE SCHEMA IF NOT EXISTS refined;   -- dado enriquecido, pronto para consumo/análise

-- ---------------------------------------------------------------------------
-- RAW: todas as linhas dos 12 arquivos, sem nenhum descarte
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.yellow_tripdata (
    vendorid                integer,
    tpep_pickup_datetime    timestamp,
    tpep_dropoff_datetime   timestamp,
    passenger_count         integer,
    trip_distance           double precision,
    ratecodeid              integer,
    store_and_fwd_flag      text,
    pulocationid            integer,
    dolocationid            integer,
    payment_type            integer,
    fare_amount             numeric(12,2),
    extra                   numeric(12,2),
    mta_tax                 numeric(12,2),
    tip_amount              numeric(12,2),
    tolls_amount            numeric(12,2),
    improvement_surcharge   numeric(12,2),
    total_amount            numeric(12,2),
    congestion_surcharge    numeric(12,2),
    airport_fee             numeric(12,2),
    source_file             text,
    file_month              integer,
    ingested_at             timestamp
);

-- ---------------------------------------------------------------------------
-- TRUSTED: apenas registros que passam nas regras de qualidade
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trusted.yellow_tripdata (
    vendorid                integer,
    tpep_pickup_datetime    timestamp,
    tpep_dropoff_datetime   timestamp,
    passenger_count         integer,
    trip_distance           double precision,
    ratecodeid              integer,
    store_and_fwd_flag      text,
    pulocationid            integer,
    dolocationid            integer,
    payment_type            integer,
    fare_amount             numeric(12,2),
    extra                   numeric(12,2),
    mta_tax                 numeric(12,2),
    tip_amount              numeric(12,2),
    tolls_amount            numeric(12,2),
    improvement_surcharge   numeric(12,2),
    total_amount            numeric(12,2),
    congestion_surcharge    numeric(12,2),
    airport_fee             numeric(12,2),
    source_file             text,
    file_month              integer,
    trusted_at              timestamp
);

-- ---------------------------------------------------------------------------
-- REFINED: tabela final (viagem a viagem, enriquecida) + resumo diário
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS refined.trips (
    vendorid                integer,
    tpep_pickup_datetime    timestamp,
    tpep_dropoff_datetime   timestamp,
    passenger_count         integer,
    trip_distance           double precision,
    ratecodeid              integer,
    store_and_fwd_flag      text,
    pulocationid            integer,
    dolocationid            integer,
    payment_type            integer,
    fare_amount             numeric(12,2),
    extra                   numeric(12,2),
    mta_tax                 numeric(12,2),
    tip_amount              numeric(12,2),
    tolls_amount            numeric(12,2),
    improvement_surcharge   numeric(12,2),
    total_amount            numeric(12,2),
    congestion_surcharge    numeric(12,2),
    airport_fee             numeric(12,2),
    source_file             text,
    file_month              integer,
    trip_date               date,              -- data de início da viagem
    dropoff_date            date,              -- data de término da viagem
    trip_duration_min       double precision,
    avg_speed_mph           double precision,
    is_plausible            boolean,           -- duração e velocidade fisicamente possíveis
    refined_at              timestamp
);

CREATE TABLE IF NOT EXISTS refined.daily_summary (
    trip_date                            date,
    trips_started                        bigint,
    trips_started_and_finished_same_day  bigint,
    total_distance_miles                 double precision,
    avg_distance_miles                   double precision,
    total_amount                         numeric(18,2)
);
