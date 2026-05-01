-- Step 1 - Database aur Schema select karo
CREATE DATABASE IF NOT EXISTS flight_analysis;
USE DATABASE flight_analysis;
CREATE SCHEMA IF NOT EXISTS staging;
USE SCHEMA staging;
USE WAREHOUSE COMPUTE_WH;

-- Step 2 - Storage Integration banao again run kroge issue aayega replace use kro agr arn change krna hai to 
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::034917377686:role/snowflake_s3_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://krish-flight/');

-- Step 3 - Stage banao
CREATE OR REPLACE STAGE gold_stage_s3
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://krish-flight/snowflake_load/'
    FILE_FORMAT = (TYPE = PARQUET);

-- Step 4 - List files check karo
LIST @gold_stage_s3;

-- DESC INTEGRATION s3_integration;

-- Step 5 - Create monthly_airline_kpi
CREATE OR REPLACE TABLE monthly_airline_kpi (
    flight_year INTEGER,
    flight_month INTEGER,
    airline_code VARCHAR,
    reporting_airline VARCHAR,
    total_flights BIGINT,
    delayed_flights BIGINT,
    total_flights_cancelled BIGINT,
    total_delay_minutes DOUBLE,
    total_diverted BIGINT,
    avg_arr_delay_minutes DOUBLE,
    median_arrival_delay DOUBLE,
    on_time_flights BIGINT,
    cancelled_flight_percentage DOUBLE,
    avg_carrier_delay DOUBLE,
    avg_weather_delay DOUBLE,
    avg_security_delay DOUBLE,
    avg_late_aircraft_delay DOUBLE,
    avg_distance_travelled DOUBLE,
    total_distance_travelled DOUBLE,
    on_time_flight_percentage DOUBLE,
    monthly_rank INTEGER
);

-- Step 6 - Create annual_route_performance
CREATE OR REPLACE TABLE annual_route_performance (
    flight_year INTEGER,
    route VARCHAR,
    origin_code VARCHAR,
    destination_code VARCHAR,
    total_flights BIGINT,
    avg_arrival_delay DOUBLE,
    avg_distance DOUBLE,
    total_delayed_flights BIGINT,
    on_time_airline_percentage DOUBLE,
    number_of_airlines BIGINT
);

-- Step 7 - Create gold_airport_departure_kpi
CREATE OR REPLACE TABLE gold_airport_departure_kpi (
    flight_year INTEGER,
    MONTH INTEGER,
    origin_code VARCHAR,
    OriginCityName VARCHAR,
    OriginState VARCHAR,
    origin_lat DOUBLE,
    origin_lon DOUBLE,
    total_departures BIGINT,
    total_cancelled_departures BIGINT,
    avg_departure_delay DOUBLE,
    departure_on_time_percentage DOUBLE,
    avg_route_distance DOUBLE,
    total_flights_operated BIGINT,
    avg_airtime DOUBLE,
    year_month VARCHAR
);

-- Step 8 - Create delay_cause_table
CREATE OR REPLACE TABLE delay_cause_table (
    flight_year INTEGER,
    month INTEGER,
    airline_code VARCHAR,
    total_min_delay DOUBLE,
    total_weather_delay_min DOUBLE,
    total_security_delay DOUBLE,
    total_late_aircraft_delay DOUBLE,
    total_late_carrier_delay DOUBLE,
    nas_delay_percentage DOUBLE,
    avg_delay_per_flight DOUBLE
);

-- Step 9 - Load Data
CREATE OR REPLACE PIPE monthly_airline_pipe
AUTO_INGEST = TRUE
AS
COPY INTO monthly_airline_kpi
FROM @gold_stage_s3/monthly_airline_kpi/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part.*\.parquet';

CREATE OR REPLACE PIPE annual_route_pipe
AUTO_INGEST = TRUE
AS
COPY INTO annual_route_performance
FROM @gold_stage_s3/gold_route_kpi/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part.*\.parquet';

CREATE OR REPLACE PIPE airport_departure_pipe
AUTO_INGEST = TRUE
AS
COPY INTO gold_airport_departure_kpi
FROM @gold_stage_s3/gold_airport_departure_kpi/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part.*\.parquet';

CREATE OR REPLACE PIPE delay_cause_pipe
AUTO_INGEST = TRUE
AS
COPY INTO delay_cause_table
FROM @gold_stage_s3/delay_cause_table/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part.*\.parquet';

-- Step 10 - Data Retention 10 days set karo
ALTER TABLE monthly_airline_kpi
SET DATA_RETENTION_TIME_IN_DAYS = 10;

ALTER TABLE annual_route_performance
SET DATA_RETENTION_TIME_IN_DAYS = 10;

ALTER TABLE gold_airport_departure_kpi
SET DATA_RETENTION_TIME_IN_DAYS = 10;

ALTER TABLE delay_cause_table
SET DATA_RETENTION_TIME_IN_DAYS = 10;

SHOW TABLES LIKE 'DELAY_CAUSE_TABLE';

-- Step 11 - Verify Data
ALTER PIPE monthly_airline_pipe REFRESH;
SELECT * FROM monthly_airline_kpi LIMIT 5;

ALTER PIPE annual_route_pipe REFRESH;
SELECT * FROM annual_route_performance LIMIT 5;

ALTER PIPE airport_departure_pipe REFRESH;
SELECT * FROM gold_airport_departure_kpi LIMIT 5;

ALTER PIPE delay_cause_pipe REFRESH;
SELECT * FROM delay_cause_table LIMIT 5;

