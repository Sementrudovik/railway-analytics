-- Staging таблица (сырые данные)
CREATE TABLE IF NOT EXISTS railway.staging_transport (
    id BIGSERIAL PRIMARY KEY,
    wagon_number VARCHAR(50),
    container_number VARCHAR(50),
    departure_date VARCHAR(20),
    cargo_code VARCHAR(50),
    departure_country VARCHAR(100),
    departure_region VARCHAR(100),
    departure_station VARCHAR(100),
    destination_country VARCHAR(100),
    destination_region VARCHAR(100),
    destination_station VARCHAR(100),
    destination_station_sng VARCHAR(100),
    number_of_containers INTEGER DEFAULT 1,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    batch_id UUID
);

-- Индексы
CREATE INDEX IF NOT EXISTS idx_staging_departure_date ON railway.staging_transport(departure_date);
CREATE INDEX IF NOT EXISTS idx_staging_batch ON railway.staging_transport(batch_id);