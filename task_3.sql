-- Создание необходимых схем и таблиц
CREATE SCHEMA IF NOT EXISTS task_3;
CREATE TABLE IF NOT EXISTS task_3.raw_downloads
(
    uuid           UUID,
    object_id      INT,
    complex_name   VARCHAR,
    developer_name VARCHAR,
    price          DECIMAL,
    square         DECIMAL,
    lat            VARCHAR,
    lon            VARCHAR,
    wall_type      VARCHAR,
    load_date      TIMESTAMP,
    city           VARCHAR,
    data_source    VARCHAR
);
CREATE TABLE IF NOT EXISTS task_3.developer
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE
);
CREATE TABLE IF NOT EXISTS task_3.wall_type
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE
);
CREATE TABLE IF NOT EXISTS task_3.city
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE
);
CREATE TABLE IF NOT EXISTS task_3.data_source
(
    id   SERIAL PRIMARY KEY,
    name VARCHAR UNIQUE
);
CREATE TABLE IF NOT EXISTS task_3.complex
(
    id           SERIAL PRIMARY KEY,
    name         VARCHAR UNIQUE,
    developer_id INT REFERENCES task_3.developer (id),
    lat          FLOAT,
    lon          FLOAT,
    wall_type_id INT REFERENCES task_3.wall_type (id),
    city_id      INT REFERENCES task_3.city (id)
);
CREATE TABLE IF NOT EXISTS task_3.downloads
(
    uuid           UUID PRIMARY KEY,
    object_id      INT,
    complex_id     INT REFERENCES task_3.complex (id),
    price          FLOAT,
    square         FLOAT,
    load_date      TIMESTAMP,
    data_source_id INT REFERENCES task_3.data_source (id)
);

-- Считывание данных из тестового датасета во временную таблицу
COPY task_3.raw_downloads FROM '/opt/app/test_dataset.csv' WITH (FORMAT csv, HEADER true, DELIMITER '|');

-- Запись данных из тестового датасета
INSERT INTO task_3.city(name)
SELECT DISTINCT city
FROM task_3.raw_downloads;

INSERT INTO task_3.data_source(name)
SELECT DISTINCT data_source
FROM task_3.raw_downloads;

INSERT INTO task_3.wall_type(name)
SELECT DISTINCT wall_type
FROM task_3.raw_downloads
WHERE developer_name NOTNULL;

INSERT INTO task_3.developer(name)
SELECT DISTINCT developer_name
FROM task_3.raw_downloads
WHERE developer_name NOTNULL;

INSERT INTO task_3.complex(name, developer_id, lat, lon, wall_type_id, city_id)
SELECT DISTINCT ON (rd.complex_name) complex_name,
                                     d.id,
                                     REPLACE(lat, ',', '.')::numeric,
                                     REPLACE(lon, ',', '.')::numeric,
                                     wt.id,
                                     c.id
FROM task_3.raw_downloads rd
         LEFT JOIN task_3.developer d ON d.name = rd.developer_name
         LEFT JOIN task_3.wall_type wt ON wt.name = rd.wall_type
         LEFT JOIN task_3.city c ON c.name = rd.city
WHERE rd.complex_name NOTNULL;

INSERT INTO task_3.downloads(uuid, object_id, complex_id, price, square, load_date, data_source_id)
SELECT uuid,
       object_id,
       c.id,
       price,
       square,
       load_date,
       d.id
FROM task_3.raw_downloads rd
         LEFT JOIN task_3.complex c ON c.name = rd.complex_name
         LEFT JOIN task_3.data_source d ON d.name = rd.data_source;

-- Удаление временной таблицы
DROP TABLE IF EXISTS task_3.raw_downloads