CREATE TABLE IF NOT EXISTS methane_data_file (methane_data_file_id SERIAL PRIMARY KEY,
    file_name VARCHAR(250) UNIQUE NOT NULL,
    metadata VARCHAR(1000) NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS methane_data (
    methane_data_id SERIAL PRIMARY KEY,
    methane_data_file_id INT REFERENCES methane_data_file(methane_data_file_id),
    recorded_at TIMESTAMP NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    methane DOUBLE PRECISION NOT NULL
);
CREATE TABLE IF NOT EXISTS methane_data_by_country (
    methane_data_by_country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(250) UNIQUE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS methane_data_by_country_by_year (
    methane_data_by_country_by_year_id SERIAL PRIMARY KEY,
    methane_data_by_country_id INT REFERENCES methane_data_by_country(methane_data_by_country_id),
    year INT NOT NULL,
    carbon_tons REAL NOT NULL
);
