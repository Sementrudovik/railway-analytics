-- db/init/02_create_schema.sql
CREATE SCHEMA IF NOT EXISTS railway;
ALTER DATABASE railway_db SET search_path TO railway, public;
