-- db/init/03_create_staging.sql
-- Staging таблица (сырые данные)
CREATE TABLE IF NOT EXISTS railway.staging_transport (
    id BIGSERIAL PRIMARY KEY,
    container_number VARCHAR(50),
    number_of_containers INTEGER DEFAULT 1,
    departure_date DATE,
    cargo_code VARCHAR(50),
    departure_country VARCHAR(100),
    departure_region VARCHAR(100),
    departure_station VARCHAR(100),
    destination_country VARCHAR(100),
    destination_region VARCHAR(100),
    destination_station VARCHAR(100),
    destination_station_sng VARCHAR(100),
    operator_name VARCHAR(100),
    payer VARCHAR(100),
    shipper_short_name VARCHAR(100),
    shipper_tax VARCHAR(100),
    consignee_short_name VARCHAR(100),
    consignee_tax VARCHAR(100),
    -- loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    -- source_file VARCHAR(500)
    -- batch_id UUID
);

-- Индексы
-- CREATE INDEX IF NOT EXISTS idx_staging_departure_date ON railway.staging_transport(departure_date);
-- CREATE INDEX IF NOT EXISTS idx_staging_batch ON railway.staging_transport(batch_id);