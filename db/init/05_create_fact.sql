-- db/init/05_create_fact.sql
-- Фактовая таблица
CREATE TABLE IF NOT EXISTS railway.fact_transport (
    fact_id BIGSERIAL PRIMARY KEY,
    date_id INTEGER REFERENCES railway.dim_date(date_id),
    departure_station_id INTEGER REFERENCES railway.dim_station(station_id),
    destination_station_id INTEGER REFERENCES railway.dim_station(station_id),
    cargo_id INTEGER REFERENCES railway.dim_cargo(cargo_id),
    container_count INTEGER,
    batch_id UUID
);