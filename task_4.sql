-- Создание view
CREATE VIEW IF NOT EXISTS enco_test.postgres_clickhouse_view AS
SELECT downloads.uuid,
       downloads.object_id,
       complex.name     AS complex_name,
       developer.name   AS developer_name,
       downloads.price,
       downloads.square,
       complex.lat,
       complex.lon,
       wall_type.name   AS wall_type,
       downloads.load_date,
       city.name        AS city,
       data_source.name AS data_source
-- Соединение необходимых данных из postgres
FROM postgresql(
     'postgres:5432',
     'enco_test',
     'downloads',
     'postgres',
     'postgres',
     'task_3'
) AS downloads
LEFT JOIN postgresql(
    'postgres:5432',
    'enco_test',
    'complex',
    'postgres',
    'postgres',
    'task_3'
) AS complex ON complex.id = downloads.complex_id
LEFT JOIN postgresql(
    'postgres:5432',
    'enco_test',
    'data_source',
    'postgres',
    'postgres',
    'task_3'
) AS data_source ON data_source.id = downloads.data_source_id
LEFT JOIN postgresql(
    'postgres:5432',
    'enco_test',
    'city',
    'postgres',
    'postgres',
    'task_3'
) AS city ON city.id = complex.city_id
LEFT JOIN postgresql(
    'postgres:5432',
    'enco_test',
    'wall_type',
    'postgres',
    'postgres',
    'task_3'
) AS wall_type ON wall_type.id = complex.wall_type_id
LEFT JOIN postgresql(
    'postgres:5432',
    'enco_test',
    'developer',
    'postgres',
    'postgres',
    'task_3'
) AS developer ON developer.id = complex.developer_id;