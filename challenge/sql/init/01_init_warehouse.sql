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
) PARTITION BY RANGE (tpep_pickup_datetime);

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
) PARTITION BY RANGE (tpep_pickup_datetime);

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
) PARTITION BY RANGE (tpep_pickup_datetime);

CREATE TABLE IF NOT EXISTS refined.daily_summary (
    trip_date                            date,
    trips_started                        bigint,
    trips_started_and_finished_same_day  bigint,
    total_distance_miles                 double precision,
    avg_distance_miles                   double precision,
    total_amount                         numeric(18,2)
);

-- ---------------------------------------------------------------------------
-- PARTICOES: raw, trusted e refined.trips sao particionadas por mes de tpep_pickup_datetime
-- (12 particoes de 2022 + uma DEFAULT). A DEFAULT recebe o que cai fora de 2022 ou tem
-- pickup nulo (a raw tem timestamps de 2002, 2008 etc.); na trusted e na refined ela fica vazia.
-- ---------------------------------------------------------------------------
DO $$
DECLARE
    parent text;
    m      int;
BEGIN
    FOREACH parent IN ARRAY ARRAY['raw.yellow_tripdata', 'trusted.yellow_tripdata', 'refined.trips'] LOOP
        FOR m IN 1..12 LOOP
            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS %s_2022_%s PARTITION OF %s FOR VALUES FROM (%L) TO (%L)',
                parent, lpad(m::text, 2, '0'), parent,
                make_date(2022, m, 1), (make_date(2022, m, 1) + interval '1 month')::date);
        END LOOP;
        EXECUTE format('CREATE TABLE IF NOT EXISTS %s_default PARTITION OF %s DEFAULT', parent, parent);
    END LOOP;
END $$;

