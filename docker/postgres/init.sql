-- Create database schemas for the medallion layers and metadata/auditing
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS audit;

-- Set default search path for administrative user
ALTER ROLE postgres SET search_path TO gold, silver, bronze, metadata, audit, public;

-- Enable UUID extension for run logs and surrogate keys
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Log completion of initialization
CREATE TABLE IF NOT EXISTS audit.init_log (
    event_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    event_desc VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO audit.init_log (event_desc) VALUES ('Database initialized with medallion schemas and uuid extension');
