-- db/init/04_create_dimensions.sql
-- Таблица измерений: даты
CREATE TABLE IF NOT EXISTS railway.dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE UNIQUE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN DEFAULT FALSE
);

-- Таблица измерений: станции
CREATE TABLE IF NOT EXISTS railway.dim_station (
    station_id SERIAL PRIMARY KEY,
    station_code VARCHAR(50) UNIQUE,
    station_name VARCHAR(200) NOT NULL,
    region VARCHAR(200),
    country VARCHAR(100)
);

-- Таблица измерений: грузы
CREATE TABLE IF NOT EXISTS railway.dim_cargo (
    cargo_id SERIAL PRIMARY KEY,
    cargo_code VARCHAR(50) UNIQUE NOT NULL
);
