USE DATABASE flight_analysis;
CREATE SCHEMA IF NOT EXISTS dimensions;
USE SCHEMA dimensions;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE TABLE airline_dimension (
    airline_sk INTEGER AUTOINCREMENT,
    airline_code VARCHAR,
    airline_name VARCHAR,
    carrier_plane BOOLEAN,
    airport_hub VARCHAR,
    effective_start_date DATE,
    effective_end_date DATE,
    is_current BOOLEAN
);

INSERT INTO airline_dimension (
    airline_code,
    airline_name,
    carrier_plane,
    airport_hub,
    effective_start_date,
    effective_end_date,
    is_current
)

WITH airline_cte AS (

    SELECT 
        v.airline_code,
        v.airline_name,
        v.carrier_plane,
        v.airport_hub

    FROM (VALUES

        ('AA', 'American Airlines',   TRUE,  'DFW'),
        ('DL', 'Delta Air Lines',     TRUE,  'ATL'),
        ('UA', 'United Airlines',     TRUE,  'ORD'),
        ('WN', 'Southwest Airlines',  TRUE,  'DAL'),
        ('B6', 'JetBlue Airways',     TRUE,  'JFK'),
        ('AS', 'Alaska Airlines',     TRUE,  'SEA'),
        ('HA', 'Hawaiian Airlines',   TRUE,  'HNL'),
        ('F9', 'Frontier Airlines',   FALSE, 'DEN'),
        ('NK', 'Spirit Airlines',     FALSE, 'FLL'),
        ('G4', 'Allegiant Air',       FALSE, 'LAS'),
        ('OO', 'SkyWest Airlines',    FALSE, 'SLC'),
        ('YV', 'Mesa Airlines',       FALSE, 'PHX'),
        ('OH', 'PSA Airlines',        FALSE, 'CLT'),
        ('YX', 'Republic Airways',    FALSE, 'IND'),
        ('MQ', 'Envoy Air',           FALSE, 'DFW'),
        ('9E', 'Endeavor Air',        FALSE, 'MSP'),
        ('QX', 'Horizon Air',         FALSE, 'PDX')

    ) AS v(airline_code, airline_name, carrier_plane, airport_hub)
)

SELECT 
    airline_code,
    airline_name,
    carrier_plane,
    airport_hub,
    '2021-01-01'::DATE AS effective_start_date,
    '9999-12-31'::DATE AS effective_end_date,
    TRUE AS is_current

FROM airline_cte;

-- Verify
SELECT * FROM airline_dimension;

-- DATE DIMENSION

CREATE OR REPLACE TABLE date_dimension (
    full_date DATE,
    year_number INTEGER,
    month_number INTEGER,
    month_name VARCHAR,
    day_of_week_number INTEGER,
    day_of_week_name VARCHAR,
    quarter_number INTEGER,
    is_weekend BOOLEAN,
    season VARCHAR,
    year_month VARCHAR
);

INSERT INTO date_dimension (

    full_date,
    year_number,
    month_number,
    month_name,
    day_of_week_number,
    day_of_week_name,
    quarter_number,
    is_weekend,
    season,
    year_month

)

WITH date_cte AS (

    SELECT
        generated_date AS full_date,

        YEAR(generated_date) AS year_number,

        MONTH(generated_date) AS month_number,

        MONTHNAME(generated_date) AS month_name,

        DAYOFWEEKISO(generated_date) AS day_of_week_number,

        DAYNAME(generated_date) AS day_of_week_name,

        QUARTER(generated_date) AS quarter_number,

        CASE
            WHEN DAYOFWEEKISO(generated_date) IN (6,7)
            THEN TRUE
            ELSE FALSE
        END AS is_weekend,

        CASE
            WHEN MONTH(generated_date) IN (12,1,2)
            THEN 'Winter'

            WHEN MONTH(generated_date) IN (3,4)
            THEN 'Spring'

            WHEN MONTH(generated_date) IN (5,6,7)
            THEN 'Summer'

            ELSE 'Rainy'
        END AS season,

        CONCAT(
            YEAR(generated_date),
            '-',
            LPAD(MONTH(generated_date), 2, '0')
        ) AS year_month

    FROM (

        SELECT
            DATEADD(
                DAY,
                SEQ4(),
                '2021-01-01'
            ) AS generated_date

        FROM TABLE(GENERATOR(ROWCOUNT => 750))
    )

    WHERE generated_date < '2023-01-01'
)

SELECT * FROM date_cte;

-- Verify
SELECT * FROM date_dimension LIMIT 20;

-- TEST PART

-- SELECT
--     ROW_NUMBER() OVER (ORDER BY SEQ4()) AS number_series
-- FROM TABLE(GENERATOR(ROWCOUNT => 100));

-- SELECT
--     DATEADD(
--         DAY,
--         SEQ4(),
--         CURRENT_DATE()
--     ) AS next_7_dates
-- FROM TABLE(GENERATOR(ROWCOUNT => 7));

--------------------------------------------------------------------------------------
-- Select airport code, name, city, latitude, longitude
SELECT DISTINCT origin_code AS airport_code, OriginCityName AS airport_name, OriginState AS city, origin_lat AS latitude, origin_lon AS longitude
FROM flight_analysis.staging.gold_airport_departure_kpi;

-- Year wise total flights departed and avg on-time flights and Avg delay minutes per year
SELECT flight_year AS year, SUM(total_departures) AS total_flights_departed,
    ROUND(AVG(departure_on_time_percentage), 2) AS avg_on_time_flights,
    ROUND(AVG(avg_departure_delay), 2)  AS avg_delay_minutes
FROM flight_analysis.staging.gold_airport_departure_kpi
GROUP BY flight_year;

-- Max operating flight for each airport
SELECT origin_code  AS airport_code, OriginCityName AS airport_name, MAX(total_flights_operated) AS max_operating_flights
FROM flight_analysis.staging.gold_airport_departure_kpi
GROUP BY origin_code, OriginCityName;

-- Airline code, name, major carrier, flight year, total flights, cancelled flights
SELECT 
    m.airline_code,
    m.reporting_airline AS airline_name,
    m.flight_year,
    d.carrier_plane  AS is_major_carrier,
    SUM(m.total_flights) AS total_flights,
    SUM(m.total_flights_cancelled)  AS total_cancelled_flights
FROM flight_analysis.staging.monthly_airline_kpi as m
LEFT JOIN flight_analysis.dimensions.airline_dimension as d
    ON m.airline_code = d.airline_code
GROUP BY m.airline_code, m.reporting_airline, m.flight_year, d.carrier_plane;

-- Avg cancellation rate per airline per year
SELECT 
    airline_code,
    reporting_airline AS airline_name,
    flight_year,
    ROUND(AVG(cancelled_flight_percentage), 2)  AS avg_cancellation_rate
FROM flight_analysis.staging.monthly_airline_kpi
GROUP BY airline_code, reporting_airline, flight_year;

-- Avg arrival delay, carrier delay, weather delay, late aircraft delay
SELECT 
    airline_code,
    flight_year,
    ROUND(AVG(avg_arr_delay_minutes), 2) AS avg_arrival_delay,
    ROUND(AVG(avg_carrier_delay), 2)  AS avg_carrier_delay,
    ROUND(AVG(avg_weather_delay), 2)  AS avg_weather_delay,
    ROUND(AVG(avg_late_aircraft_delay), 2)  AS avg_late_aircraft_delay
FROM flight_analysis.staging.monthly_airline_kpi
GROUP BY airline_code, flight_year;

-- Join monthly_airline_kpi with airline_dimension
-- Group by code, name, year, month, reporting airline
SELECT 
    m.airline_code,
    d.airline_name, d.carrier_plane  AS is_major_carrier, d.airport_hub,
    m.flight_year, m.flight_month,m.reporting_airline,
    
    SUM(m.total_flights) AS total_flights, SUM(m.total_flights_cancelled) AS total_cancelled_flights,
    ROUND(AVG(m.cancelled_flight_percentage),2) AS avg_cancellation_rate,
    ROUND(AVG(m.avg_arr_delay_minutes), 2) AS avg_arrival_delay,
    ROUND(AVG(m.avg_carrier_delay), 2) AS avg_carrier_delay,
    ROUND(AVG(m.avg_weather_delay), 2) AS avg_weather_delay,
    ROUND(AVG(m.avg_late_aircraft_delay), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(m.on_time_flight_percentage), 2) AS avg_on_time_percentage
    
FROM flight_analysis.staging.monthly_airline_kpi as m
LEFT JOIN flight_analysis.dimensions.airline_dimension as d
    ON m.airline_code = d.airline_code
GROUP BY
    m.airline_code,
    d.airline_name, d.carrier_plane, d.airport_hub,
    m.flight_year, m.flight_month, m.reporting_airline;
------------------------------------------------------------
-- View 1 - Airport Departure KPI View
CREATE OR REPLACE VIEW vw_airport_departure_kpi AS
SELECT DISTINCT
    origin_code AS airport_code, OriginCityName AS airport_name, OriginState AS city, origin_lat AS latitude,
    origin_lon  AS longitude, flight_year  AS year,
    SUM(total_departures) AS total_flights_departed,
    ROUND(AVG(departure_on_time_percentage),2) AS avg_on_time_flights,
    ROUND(AVG(avg_departure_delay), 2) AS avg_delay_minutes,
    MAX(total_flights_operated)  AS max_operating_flights
FROM flight_analysis.staging.gold_airport_departure_kpi
GROUP BY
    origin_code,
    OriginCityName,
    OriginState,
    origin_lat,
    origin_lon,
    flight_year
ORDER BY flight_year;

-- View 2 - Airline Performance View
CREATE OR REPLACE VIEW vw_airline_performance AS
SELECT 
    m.airline_code,
    d.airline_name, d.carrier_plane AS is_major_carrier, d.airport_hub,
    m.flight_year, m.flight_month, m.reporting_airline,
    SUM(m.total_flights) AS total_flights,
    SUM(m.total_flights_cancelled)  AS total_cancelled_flights,
    ROUND(AVG(m.cancelled_flight_percentage),2) AS avg_cancellation_rate,
    ROUND(AVG(m.avg_arr_delay_minutes), 2)  AS avg_arrival_delay,
    ROUND(AVG(m.avg_carrier_delay), 2) AS avg_carrier_delay,
    ROUND(AVG(m.avg_weather_delay), 2) AS avg_weather_delay,
    ROUND(AVG(m.avg_late_aircraft_delay), 2) AS avg_late_aircraft_delay,
    ROUND(AVG(m.on_time_flight_percentage), 2)  AS avg_on_time_percentage
FROM flight_analysis.staging.monthly_airline_kpi as m
LEFT JOIN flight_analysis.dimensions.airline_dimension as d
    ON m.airline_code = d.airline_code
GROUP BY
    m.airline_code,
    d.airline_name, d.carrier_plane, d.airport_hub,
    m.flight_year, m.flight_month, m.reporting_airline;

-- Verify Views
SELECT * FROM vw_airport_departure_kpi LIMIT 5;
SELECT * FROM vw_airline_performance LIMIT 5;