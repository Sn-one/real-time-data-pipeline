-- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create a hypertable for time-series data
-- Replace 'your_hypertable_name' with your desired table name
SELECT create_hypertable('your_hypertable_name', 'timestamp');

-- Create a table for storing time-series data
-- Replace 'your_timeseries_table_name' with your desired table name
CREATE TABLE your_timeseries_table_name (
    time TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION
);
