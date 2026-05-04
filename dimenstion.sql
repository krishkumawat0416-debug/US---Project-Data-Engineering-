USE DATABASE flight_analysis;
CREATE SCHEMA IF NOT EXISTS dimensions;
USE SCHEMA dimensions;
USE WAREHOUSE COMPUTE_WH;

CREATE OR REPLACE TABLE airline_dimension (
    airline_sk INTEGER AUTOINCREMENT,
    airline_code VARCHAR,
    airline_name VARCHAR,
    carrier_plane VARCHAR,
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

SELECT DISTINCT airline_code,
    CASE airline_code
        WHEN 'F9' THEN 'Frontier Airlines'
        WHEN 'YV' THEN 'Mesa Airlines'
        WHEN 'AA' THEN 'American Airlines'
        WHEN 'NK' THEN 'Spirit Airlines'
        WHEN 'OH' THEN 'PSA Airlines'
        WHEN 'YX' THEN 'Republic Airways'
        WHEN 'AS' THEN 'Alaska Airlines'
        WHEN 'MQ' THEN 'Envoy Air'
        WHEN 'DL' THEN 'Delta Air Lines'
        WHEN 'UA' THEN 'United Airlines'
        WHEN '9E' THEN 'Endeavor Air'
        WHEN 'HA' THEN 'Hawaiian Airlines'
        WHEN 'QX' THEN 'Horizon Air'
        WHEN 'OO' THEN 'SkyWest Airlines'
        WHEN 'WN' THEN 'Southwest Airlines'
        WHEN 'B6' THEN 'JetBlue Airways'
        WHEN 'G4' THEN 'Allegiant Air'
        ELSE airline_code

    END AS airline_name,
    NULL AS carrier_plane,
    NULL AS airport_hub,
    CURRENT_DATE() AS effective_start_date,
    NULL AS effective_end_date,
    TRUE AS is_current

FROM flight_analysis.staging.monthly_airline_kpi;

SELECT * FROM airline_dimension;

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
    quarter_number

)

SELECT
    generated_date AS full_date,
    YEAR(generated_date) AS year_number,
    MONTH(generated_date) AS month_number,
    MONTHNAME(generated_date) AS month_name,
    DAYOFWEEKISO(generated_date) AS day_of_week_number,
    DAYNAME(generated_date) AS day_of_week_name,
    QUARTER(generated_date) AS quarter_number
FROM (
    SELECT
        DATEADD(
            DAY,
            SEQ4(),
            '2021-01-01'
        ) AS generated_date

    FROM TABLE(GENERATOR(ROWCOUNT => 750))
)
WHERE generated_date < '2023-01-01';

SELECT * FROM date_dimension LIMIT 20;

UPDATE date_dimension
SET is_weekend =

CASE
    WHEN day_of_week_number IN (6,7)
    THEN TRUE
    ELSE FALSE
END;

SELECT
    full_date,
    day_of_week_number,
    day_of_week_name,
    is_weekend

FROM date_dimension LIMIT 20;


UPDATE date_dimension
SET season =

CASE

    WHEN month_number IN (12,1,2)
    THEN 'Winter'

    WHEN month_number IN (3,4)
    THEN 'Spring'

    WHEN month_number IN (5,6,7)
    THEN 'Summer'

    ELSE 'Rainy'

END;

SELECT
    full_date,
    month_number,
    month_name,
    season

FROM date_dimension;

UPDATE date_dimension
SET year_month =

CONCAT(
    year_number,
    '-',
    LPAD(month_number, 2, '0')
);

SELECT
    full_date,
    year_number,
    month_number,
    year_month

FROM date_dimension;

SELECT * FROM date_dimension;

-- # Test Part 
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