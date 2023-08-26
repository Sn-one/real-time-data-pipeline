-- Create the database
CREATE DATABASE your_database_name;

-- Connect to the database
\c your_database_name;

-- Create a table for storing processed data
CREATE TABLE processed_data (
    highway TEXT,
    type TEXT,
    color TEXT,
    direction TEXT,
    speed INT,
    timestamp INT
);
