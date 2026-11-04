-- Teste rapido do particionamento (roda no CI antes da carga; nao deixa nada no banco).
-- Confere: roteamento para a particao certa, NULL e anos fora de 2022 na DEFAULT, e TRUNCATE do pai.
BEGIN;

INSERT INTO raw.yellow_tripdata (tpep_pickup_datetime, file_month) VALUES
    ('2022-06-17 10:00:00', 6),
    ('2022-12-31 23:59:59', 12),
    ('2002-12-31 23:00:00', 12),
    (NULL, 1);

DO $$
DECLARE
    n_jun int; n_dec int; n_default int; n_total int;
BEGIN
    SELECT count(*) INTO n_jun     FROM raw.yellow_tripdata_2022_06;
    SELECT count(*) INTO n_dec     FROM raw.yellow_tripdata_2022_12;
    SELECT count(*) INTO n_default FROM raw.yellow_tripdata_default;
    SELECT count(*) INTO n_total   FROM raw.yellow_tripdata;
    IF n_jun <> 1 OR n_dec <> 1 OR n_default <> 2 OR n_total <> 4 THEN
        RAISE EXCEPTION 'roteamento de particoes incorreto: jun=%, dez=%, default=%, total=%',
            n_jun, n_dec, n_default, n_total;
    END IF;
END $$;

TRUNCATE TABLE raw.yellow_tripdata;

DO $$
BEGIN
    IF (SELECT count(*) FROM raw.yellow_tripdata) <> 0 THEN
        RAISE EXCEPTION 'TRUNCATE do pai nao esvaziou as particoes';
    END IF;
END $$;

ROLLBACK;
