-- Verify the database exists
SHOW DATABASES;

-- Verify the table exists
SHOW TABLES FROM nyc_taxi;

-- Check the table structure
DESCRIBE TABLE nyc_taxi.trips_small;

-- Check if table has any data
SELECT count() FROM nyc_taxi.trips_small;
