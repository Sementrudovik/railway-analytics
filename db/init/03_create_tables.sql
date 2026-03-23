CREATE TABLE IF NOT EXISTS railway_analytics (
    id SERIAL PRIMARY KEY,
    wagon_number DECIMAL(8),
    container_number VARCHAR(11),
    date_of_departure DATE,
    cargo_code DECIMAL(12),
    departure_country VARCHAR(20),
    departure_region VARCHAR(20),
    departure_station VARCHAR(20),
    destination_station VARCHAR(20),
    destination_region VARCHAR(20),
    destination_station_sng VARCHAR(20),
    number_of_containers DECIMAL(8),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source_file VARCHAR(500),
    batch_id UUID
);