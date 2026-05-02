<div align="center">

# ✈️ US Domestic Flight Delay Analytics Platform

### A Production-Grade End-to-End Data Engineering Project

![Python](https://img.shields.io/badge/Python-3.x-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-PySpark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![Databricks](https://img.shields.io/badge/Databricks-Notebook-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-ACID-00ADD8?style=for-the-badge&logo=delta&logoColor=white)
![AWS S3](https://img.shields.io/badge/AWS-S3-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-Serving%20Layer-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)

**~50M rows · 7GB raw data · 36 months (2021–2023) · Medallion Architecture**

*Bureau of Transportation Statistics (BTS) → S3 → Databricks → Snowflake → Power BI*

</div>

---

## 📌 Project Summary

This project builds a **complete, production-grade Data Lakehouse pipeline** for analyzing U.S. domestic airline on-time performance. Raw CSV data from the **Bureau of Transportation Statistics (BTS)** flows through a **Medallion Architecture** (Bronze → Silver → Gold) in Databricks using **Apache Spark** and **Delta Lake**, before being served via **Snowflake** to a **Power BI dashboard**.

The pipeline handles over **50 million flight records** spanning 36 months, applying rigorous **data quality checks**, **transformations**, and **aggregations** at each layer — making it a real-world reference implementation of a modern Data Lakehouse.

| Detail | Value |
|---|---|
| **Data Source** | Bureau of Transportation Statistics (BTS) TranStats |
| **Dataset Size** | ~7 GB raw CSV · ~50M rows across 36 months |
| **Date Range** | 2021 – 2023 |
| **Architecture** | Medallion (Bronze → Silver → Gold) Data Lakehouse |
| **Processing Engine** | Apache Spark (PySpark) on Databricks Community Edition |
| **Storage Layer** | Amazon S3 + Delta Lake (ACID, time travel, schema enforcement) |
| **Serving Layer** | Snowflake (auto-ingest pipes, data retention, staging tables) |
| **Reporting Layer** | Power BI Desktop (DirectQuery or Import mode) |
| **Total Cost** | ~$5–$20 (mostly free/trial tier tools) |

---

## 🗺️ End-to-End Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        US DOMESTIC FLIGHT DELAY PIPELINE                         │
└──────────────────────────────────────────────────────────────────────────────────┘

  [BTS Website]          [Amazon S3]              [Databricks]           [Snowflake]
       │                     │                        │                      │
  download.py  ──────▶  Raw CSVs Land          Phase 1 (Bronze)    ◀──  Phase 4 SQL
  upload.py    ──────▶  s3://krish-flight/     Phase 2 (Silver)         Staging Tables
                             │                  Phase 3 (Gold)  ──────▶  Parquet Load
                             │                        │                      │
                        Delta Tables             4 Gold KPI Tables      Power BI
                        partitioned by          written as Parquet     Dashboard
                        year / month            to S3 for Snowflake
```

### Medallion Layers

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│    BRONZE    │───▶│    SILVER    │───▶│     GOLD     │───▶│  SNOWFLAKE   │
│              │    │              │    │              │    │              │
│ Raw CSV data │    │ Cleaned,     │    │ Pre-aggregated│   │ Staging +    │
│ + DQ flags   │    │ deduplicated,│    │ KPI tables   │    │ Reporting    │
│ + partitions │    │ enriched,    │    │ for BI       │    │ views for    │
│              │    │ typed        │    │              │    │ Power BI     │
└──────────────┘    └──────────────┘    └──────────────┘    └──────────────┘
  ~19–20M rows        ~18.5–19.5M       4 tables            4 Snowflake
  ~4.5 GB Delta        rows, ~3.8 GB    (<5 MB each)        Pipes (auto-ingest)
```

---

## 📁 Repository Structure

```
flight-delay-analytics/
│
├── download.py                          ← Phase 0A: Download BTS ZIPs from website
├── upload.py                            ← Phase 0B: Upload CSVs to S3
│
├── notebooks/
│   ├── Phase-1_Bronze.ipynb             ← Schema definition + raw ingestion
│   ├── Phase-2_Bronze_to_Silver.ipynb   ← DQ flags + Bronze Delta write + Airport data
│   ├── Phase-3_Silver.ipynb             ← Full Silver transformations + enrichment
│   └── Phase-4_Gold_Snowflake.ipynb     ← Gold aggregations + Snowflake load
│
├── sql/
│   └── s3_stages.sql                    ← Snowflake DDL: stages, tables, pipes
│
└── README.md                            ← This file
```

---

## 🔢 Pipeline Execution Order

| Step | File | What It Does | Runtime |
|:---:|---|---|---|
| **0A** | `download.py` | Downloads monthly BTS ZIP files from transtats.bts.gov, extracts CSVs | 45–90 min |
| **0B** | `upload.py` | Uploads CSVs to S3 under `flight-data/YYYY/MonthMM/` structure | 5–15 min |
| **1** | `Phase-1_Bronze.ipynb` | Defines 110-column schema, first CSV read, schema verification | — |
| **2** | `Phase-2_Bronze_to_Silver.ipynb` | DQ flags, Bronze Delta write, Airport reference ingestion | 25–40 min |
| **3** | `Phase-3_Silver.ipynb` | Full transformations, enrichment, Silver Delta write | 15–25 min |
| **4** | `Phase-4_Gold_Snowflake.ipynb` | Gold KPI aggregations, Parquet export, Snowflake load | 10–15 min |
| **5** | `s3_stages.sql` | Snowflake DDL: creates tables, S3 integration, auto-ingest pipes | 5 min |

---

## 📥 Phase 0 — Data Acquisition

### `download.py` — BTS Data Downloader

Downloads monthly flight performance ZIP files from BTS, extracts the CSV inside each, and saves locally.

```python
BTS_WEBSITE = "https://transtats.bts.gov/PREZIP/"
YEARS_TO_DOWNLOAD = [2021, 2022]
LOCAL_SAVE_FOLDER = "./downloaded_files"
```

**Key behaviours:**
- Builds the exact BTS filename format: `On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip`
- Uses `stream=True` to download in **1 MB chunks** — avoids loading large files into RAM
- **Skips months already downloaded** (`os.path.exists` check) — safe to re-run
- Extracts CSV from ZIP, deletes the ZIP to save disk space
- Runs a **row count quality check** — flags files with fewer than 400,000 rows as suspicious

### `upload.py` — S3 Uploader

Uploads the downloaded CSVs to Amazon S3 with a structured folder hierarchy.

```python
# S3 path built from filename
s3_key = f"flight-data/{year}/Month{month}/{file_name}"
# e.g. → flight-data/2021/Month03/On_Time_...2021_3.csv
```

**Key behaviours:**
- Parses `year` and `month` directly from the BTS filename
- Zero-pads month to 2 digits: `1` → `01`
- Uses `boto3.client.upload_file()` — handles large files automatically
- Prints a final summary: total files, uploaded, failed

---

## 🥉 Phase 1 — Schema Definition & First Read

**Notebook:** `Phase-1_Bronze.ipynb`

This notebook defines the **110-column PySpark schema** and performs exploratory reads from S3.

### Why Not `inferSchema`?

> **BTS CSVs end every row with a trailing comma**, creating an unnamed 111th column. `inferSchema` is also slow on 7 GB of data. An explicit schema is always used in production — it's faster, safer, and self-documenting.

### Key Actions

```python
# First read — no schema (exploration only)
df = spark.read.format("csv").option("header", "true") \
    .load("s3://krish-flight/flight_data//*/*/*")

df.columns  # inspect raw column names
```

```python
# Production read — typed schema applied
df_full = spark.read.format("csv") \
    .option("header", "true") \
    .schema(flight_schema) \
    .load("s3://krish-flight/flight_data//*/*/*")
```

The schema covers **10 column groups**: Flight Date/Time, Airline Info, Origin Airport, Destination Airport, Departure Timing, Arrival Timing, Taxi Times, Cancellation & Diversion, Delay Causes, and Diversion Details.

> **Note:** `Flight_Number_Reporting_Airline` is intentionally defined as `IntegerType` here — this is flagged in the code as a known issue to be corrected in Silver (it should be `StringType` since leading zeros matter for flight numbers).

---

## 🥉 Phase 2 — Bronze Layer + Airport Reference

**Notebook:** `Phase-2_Bronze_to_Silver.ipynb`

### Part A — Bronze Flight Data

#### Step 1: Column Selection (36 of 110)

Only **36 essential columns** are selected from the 110-column raw dataset — keeping the pipeline lean while retaining all necessary business fields.

```python
cols_36 = [
    "FlightDate", "Reporting_Airline", "IATA_CODE_Reporting_Airline",
    "Tail_Number", "Flight_Number_Reporting_Airline",
    "Origin", "OriginCityName", "OriginState",
    "Dest", "DestCityName", "DestState",
    "CRSDepTime", "DepTime", "DepDelay", "DepDelayMinutes", "DepDel15",
    "DepartureDelayGroups", "CRSArrTime", "ArrTime", "ArrDelayMinutes",
    "ArrivalDelayGroups", "Cancelled", "CancellationCode", "Diverted",
    "CRSElapsedTime", "ActualElapsedTime", "AirTime", "Flights",
    "Distance", "DistanceGroup",
    "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
    "LateAircraftDelay", "_corrupt_record"
]
```

#### Step 2: PERMISSIVE Read with Null Handling

```python
df_full = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_corrupt_record") \
    .option("nullValue", "") \
    .option("emptyValue", "") \
    .schema(flight_schema) \
    .load("s3://krish-flight/flight_data//*/*/*")
```

> **`PERMISSIVE` mode** captures bad rows in `_corrupt_record` instead of crashing the entire job — essential for production pipelines where data quality cannot be guaranteed at the source.

#### Step 3: Null Percentage Check

```python
# Checks null % across 6 key columns
cols_check = ["Distance", "DepDelay", "Reporting_Airline", "Origin", "Cancelled", "Dest"]
```

A dictionary stores each column's null percentage — used to decide if a column is reliable enough to use downstream.

#### Step 4: Data Quality (DQ) Flag Columns

Seven additive DQ flag columns are added at the Bronze layer. **Bronze never discards data — it only flags it.**

| **Flag Column** | **Logic** | **What It Catches** |
|---|---|---|
| `FlightDate_null` | `FlightDate IS NULL → 1` | Missing date rows |
| `Reporting_Airline_null` | `Airline IS NULL → 1` | Missing carrier |
| `Origin_null` | `Origin IS NULL → 1` | Missing origin airport |
| `Dest_null` | `Dest IS NULL → 1` | Missing destination |
| `dep_delay_flag` | `DepDelay NOT BETWEEN -120 AND 1440 → 0` | Out-of-range delay values |
| `same_origin_destination` | `Origin == Dest → 1` | Same origin/destination |
| `is_domestic_flag` | `Distance NOT BETWEEN 1 AND 10000 → 0` | Invalid distance values |

#### Step 5: Date Partitioning + Delta Write

```python
bronze_df = bronze_df \
    .withColumn("FlightDate", to_date("FlightDate")) \
    .withColumn("year", year("FlightDate")) \
    .withColumn("month", month("FlightDate"))

bronze_df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("year", "month") \
    .save("s3://krish-flight/flight_bronze/")
```

> **Partitioning by `year` / `month`** enables Spark to skip irrelevant partitions entirely — dramatically reduces query time for time-filtered queries.

#### Step 6: Register as Delta Table

```sql
CREATE DATABASE IF NOT EXISTS flight_analytics;

CREATE OR REPLACE TABLE flight_analytics.bronze_flight
USING DELTA AS
SELECT * FROM delta.`s3://krish-flight/flight_bronze/`;
```

---

### Part B — Airport Reference Data

```python
# Schema for airport JSON reference
schema = StructType([
    StructField("code", StringType(), True),
    StructField("iata", StringType(), True),
    StructField("name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("tz", StringType(), True)
])

# Read multiline JSON from S3
df = spark.read.option("multiline", "true").schema(schema) \
    .json("s3://krish-flight/Airport-Flight/airports_multiline.json")

# Filter US airports only
df_us = df.filter(col("country") == "US")

# Save as Delta table
df_us.write.format("delta").mode("overwrite") \
    .save("s3://krish-flight/delta/Airport")
```

---

## 🥈 Phase 3 — Silver Layer Transformations

**Notebook:** `Phase-3_Silver.ipynb`

Silver is the **transformation and enrichment layer**. The Bronze Delta table is loaded and progressively enhanced with **75 final columns** across multiple transformation steps.

### 3.1 Load Bronze & Add Composite DQ Score

```python
df = spark.read.format("delta").load("s3://krish-flight/flight_bronze/")

# Sum all 7 DQ flag columns into one score
df = df.withColumn("total_null_count",
    col("FlightDate_null") + col("Reporting_Airline_null") +
    col("Origin_null") + col("Dest_null") +
    col("dep_delay_flag") + col("same_origin_destination") + col("is_domestic_flag")
)

# Label rows as VALID or INVALID
df_final = df.withColumn("is_data_clean",
    when(col("total_null_count") <= 2, "VALID").otherwise("INVALID")
)
```

### 3.2 Deduplication with Window Function

```python
window_spec = Window.partitionBy(
    "FlightDate", "Reporting_Airline",
    "Flight_Number_Reporting_Airline", "Origin", "Dest"
).orderBy("FlightDate")

df_window = df.withColumn("row_num", row_number().over(window_spec))
# Keep only row_num == 1 per unique flight key
```

### 3.3 Time Standardisation

**HHMM → HH:MM format:**

```python
# e.g. 830 → "08:30"
hours = floor(col("CRSDepTime") / 100).cast("long")
minutes = col("CRSDepTime") % 100

df = df.withColumn("crs_dep_time_std",
    concat(
        when(hours < 10, lit("0")).otherwise(lit("")), hours.cast("string"),
        lit(":"),
        when(minutes < 10, lit("0")).otherwise(lit("")), minutes.cast("string")
    )
)
```

**HHMM → Total minutes since midnight (null-safe):**

```python
def hours_to_minutes_safe(column):
    return when(column.isNull(), None) \
           .when(column == 2400, 0) \
           .otherwise(floor(column / 100) * 60 + column % 100)

df = df \
    .withColumn("crs_dep_minutes_safe", hours_to_minutes_safe(col("CRSDepTime"))) \
    .withColumn("crs_arr_minutes_safe", hours_to_minutes_safe(col("CRSArrTime")))
```

### 3.4 Boolean Flag Columns

```python
df = df \
    .withColumn("is_cancelled",         when(col("Cancelled") == 1, True).otherwise(False)) \
    .withColumn("is_diverted",          when(col("Diverted") == 1, True).otherwise(False)) \
    .withColumn("is_departure_delayed", when(col("DepDel15") == 1, True).otherwise(False)) \
    .withColumn("is_arrival_delayed",   when(col("ArrivalDelayGroups") > 0, True).otherwise(False))
```

### 3.5 Arrival Delay Category Buckets

```python
df = df.withColumn("arrival_delay_category_bucket",
    when(col("Cancelled") == 1,                                             "Cancelled")
    .when(col("ArrDelayMinutes") < 0,                                       "Early")
    .when((col("ArrDelayMinutes") >= 0)  & (col("ArrDelayMinutes") <= 14),  "On Time")
    .when((col("ArrDelayMinutes") >= 15) & (col("ArrDelayMinutes") <= 44),  "Minor Delay")
    .when((col("ArrDelayMinutes") >= 45) & (col("ArrDelayMinutes") <= 120), "Major Delay")
    .when(col("ArrDelayMinutes") > 120,                                     "Severe Delay")
    .otherwise(None)
)
```

### 3.6 Date Parts & Standardised Codes

```python
df = df \
    .withColumn("flight_year",           year(col("FlightDate"))) \
    .withColumn("flight_month",          month(col("FlightDate"))) \
    .withColumn("flight_quarter",        quarter(col("FlightDate"))) \
    .withColumn("day_of_week",           dayofweek(col("FlightDate"))) \
    .withColumn("airline_code",          upper(col("IATA_CODE_Reporting_Airline"))) \
    .withColumn("origin_code",           upper(col("Origin"))) \
    .withColumn("destination_code",      upper(col("Dest"))) \
    .withColumn("cancellation_reason",
        when(col("CancellationCode") == "A", "Carrier")
        .when(col("CancellationCode") == "B", "Weather")
        .when(col("CancellationCode") == "C", "National_Security")
        .when(col("CancellationCode") == "D", "Security_Concern")
        .otherwise(None)
    )
```

### 3.7 Custom UDF — Flight Number Character Extractor

```python
def extract_flight_chars(flight_number):
    if flight_number is None:
        return None
    flight_str = str(flight_number)
    result = ""
    if len(flight_str) >= 1: result += flight_str[0]
    if len(flight_str) >= 4: result += flight_str[3]
    if len(flight_str) >= 7: result += flight_str[6]
    return result

extract_flight_chars_udf = udf(extract_flight_chars, StringType())
df_silver = df_silver.withColumn("flight_number_extract",
    extract_flight_chars_udf(col("Flight_Number_Reporting_Airline")))
```

### 3.8 Airport Coordinate Enrichment (Broadcast Join)

```python
# Origin coordinates
df_origin_coords = df_Airport.select(
    col("iata").alias("origin_iata"),
    col("lat").alias("origin_lat"),
    col("lon").alias("origin_lon")
)
df_silver = df_silver.join(
    broadcast(df_origin_coords),
    df_silver["Origin"] == df_origin_coords["origin_iata"], "left"
).drop("origin_iata")

# Destination coordinates — same pattern with dest_lat, dest_lon
```

> **`broadcast()` hint** sends the small airport reference (~900 rows) to all worker nodes — completely avoids shuffling 7 GB of flight data across the network.

### 3.9 Haversine Distance (Great-Circle)

```python
def add_haversine_distance(df, lat1, lon1, lat2, lon2):
    return df.withColumn("haversine_distance_km",
        round(
            acos(
                sin(col(lat1) * lit(PI / 180)) * sin(col(lat2) * lit(PI / 180)) +
                cos(col(lat1) * lit(PI / 180)) * cos(col(lat2) * lit(PI / 180)) *
                cos(col(lon2) * lit(PI / 180) - col(lon1) * lit(PI / 180))
            ) * lit(6371.0), 5
        )
    )
```

> Used for **cross-validating BTS scheduled distances** with actual great-circle geometry — flags routes where BTS distance deviates more than 15% from computed distance.

### 3.10 Silver Delta Write

```python
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("delta.dataSkippingNumIndexedCols", 5) \
    .partitionBy("year", "month") \
    .save("s3://krish-flight/delta/Flight/df_silver/")
```

> **`dataSkippingNumIndexedCols=5`** indexes the first 5 columns for data skipping — speeds up filtered reads by **60–80%** on large Delta tables.

---

## 🥇 Phase 4 — Gold Layer + Snowflake Load

**Notebook:** `Phase-4_Gold_Snowflake.ipynb`

Reads Silver, benchmarks caching, then produces **4 Gold KPI tables** before loading them into Snowflake.

### 4.1 Caching Benchmark

```python
# Without cache
start = time.time()
df_silver.count()
print("Without cache:", round(time.time() - start, 2), "seconds")

# Create temp view (Serverless-compatible alternative to .cache())
df_silver.createOrReplaceTempView("df_silver_cached")

# With cache (second scan is faster)
start = time.time()
df_silver.count()
print("With cache:", round(time.time() - start, 2), "seconds")
```

### 4.2 Gold Table 1 — `monthly_airline_kpi`

Groups by `flight_year`, `flight_month`, `airline_code`, `Reporting_Airline` and computes:

| **Metric** | **Calculation** |
|---|---|
| `total_flights` | `count(*)` |
| `delayed_flights` | `sum(is_arrival_delayed == True)` |
| `total_flights_cancelled` | `sum(is_cancelled == True)` |
| `total_delay_minutes` | `sum(DepDelayMinutes)` |
| `total_diverted` | `sum(is_diverted == True)` |
| `avg_arr_delay_minutes` | `avg(ArrDelayMinutes)` for non-cancelled only |
| `median_arrival_delay` | `percentile_approx(ArrDelayMinutes, 0.5)` |
| `on_time_flights` | `sum(~is_cancelled AND ~is_arrival_delayed)` |
| `cancelled_flight_percentage` | `cancelled / total * 100` |
| `avg_carrier_delay` | `avg(CarrierDelay)` |
| `avg_weather_delay` | `avg(WeatherDelay)` |
| `avg_security_delay` | `avg(SecurityDelay)` |
| `avg_late_aircraft_delay` | `avg(LateAircraftDelay)` |
| `avg_distance_travelled` | `avg(Distance)` |
| `total_distance_travelled` | `sum(Distance)` |
| `on_time_flight_percentage` | `on_time / total * 100` |
| `monthly_rank` | `RANK() OVER (PARTITION BY year, month ORDER BY total_flights DESC)` |

### 4.3 Gold Table 2 — `gold_route_kpi`

```python
df_silver_route = df_silver.withColumn("route",
    concat(col("origin_code"), lit("/"), col("destination_code"))
)

df_gold_route = df_silver_route.groupBy(
    "flight_year", "route", "origin_code", "destination_code"
).agg(
    count("*").alias("total_flights"),
    round(avg("ArrDelayMinutes"), 2).alias("avg_arrival_delay"),
    round(avg("Distance"), 2).alias("avg_distance"),
    sum(when(col("is_arrival_delayed") == True, 1).otherwise(0)).alias("total_delayed_flights"),
    round(...).alias("on_time_airline_percentage"),
    countDistinct("airline_code").alias("number_of_airlines")
)
```

### 4.4 Gold Table 3 — `gold_airport_departure_kpi`

Groups by `flight_year`, `flight_month`, `origin_code`, city, state, lat, lon and computes **departure-level metrics**: total departures, cancellations, avg departure delay, on-time percentage, avg distance, avg airtime. Also adds a `year_month` string column (e.g. `"2021-3"`) for time-series charting.

### 4.5 Gold Table 4 — `delay_cause_table`

Filters to **non-cancelled flights with positive delay**, then groups by `flight_year`, `month`, `airline_code`:

```python
df_delay_cause = df_delay.groupBy("flight_year", "month", "airline_code").agg(
    round(sum("ArrDelayMinutes"), 2).alias("total_min_delay"),
    round(sum("WeatherDelay"), 2).alias("total_weather_delay_min"),
    round(sum("SecurityDelay"), 2).alias("total_security_delay"),
    round(sum("LateAircraftDelay"), 2).alias("total_late_aircraft_delay"),
    round(sum("CarrierDelay"), 2).alias("total_late_carrier_delay"),
    round(sum("NASDelay") / sum("ArrDelayMinutes") * 100, 2).alias("nas_delay_percentage"),
    round(avg("ArrDelayMinutes"), 2).alias("avg_delay_per_flight")
)
```

### 4.6 Export as Parquet for Snowflake

> **Why Parquet?** Databricks Serverless does not support direct Snowflake writes. Parquet is the ideal intermediate format — it is compressed, columnar, and natively supported by both Spark and Snowflake.

```python
# All 4 Gold tables written as Parquet to S3 staging area
df_gold_ranked.write.format("parquet").mode("overwrite") \
    .save("s3://krish-flight/snowflake_load/monthly_airline_kpi/")

df_gold_route.write.format("parquet").mode("overwrite") \
    .save("s3://krish-flight/snowflake_load/gold_route_kpi/")

df_gold_airport.write.format("parquet").mode("overwrite") \
    .save("s3://krish-flight/snowflake_load/gold_airport_departure_kpi/")

df_delay_cause.write.format("parquet").mode("overwrite") \
    .save("s3://krish-flight/snowflake_load/delay_cause_table/")
```

---

## ❄️ Phase 5 — Snowflake Setup

**Script:** `s3_stages.sql`

### Step-by-Step SQL Execution

**Step 1 — Database & Schema:**

```sql
CREATE DATABASE IF NOT EXISTS flight_analysis;
USE DATABASE flight_analysis;
CREATE SCHEMA IF NOT EXISTS staging;
USE WAREHOUSE COMPUTE_WH;
```

**Step 2 — S3 Storage Integration:**

```sql
CREATE STORAGE INTEGRATION s3_integration
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::XXXXXXXXXXXX:role/snowflake_s3_role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://krish-flight/');
```

**Step 3 — External Stage (Parquet):**

```sql
CREATE OR REPLACE STAGE gold_stage_s3
    STORAGE_INTEGRATION = s3_integration
    URL = 's3://krish-flight/snowflake_load/'
    FILE_FORMAT = (TYPE = PARQUET);
```

**Steps 4 — Create 4 Staging Tables** with Data Retention of 10 days each:

```sql
-- Example: monthly_airline_kpi
CREATE OR REPLACE TABLE monthly_airline_kpi (
    flight_year INTEGER, flight_month INTEGER,
    airline_code VARCHAR, reporting_airline VARCHAR,
    total_flights BIGINT, ...
    monthly_rank INTEGER
);

ALTER TABLE monthly_airline_kpi SET DATA_RETENTION_TIME_IN_DAYS = 10;
```

> Same pattern for: `annual_route_performance`, `gold_airport_departure_kpi`, `delay_cause_table`

**Step 5 — Auto-Ingest Pipes:**

```sql
CREATE OR REPLACE PIPE monthly_airline_pipe
AUTO_INGEST = TRUE AS
COPY INTO monthly_airline_kpi
FROM @gold_stage_s3/monthly_airline_kpi/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
PATTERN = '.*part.*\.parquet';
```

> **`AUTO_INGEST = TRUE`** means Snowflake automatically triggers ingestion when new Parquet files appear in S3 — via **SQS event notifications**. Zero manual intervention needed once pipes are set up.

**Step 6 — Verify:**

```sql
ALTER PIPE monthly_airline_pipe REFRESH;
SELECT * FROM monthly_airline_kpi LIMIT 5;
```

---

## 📊 Power BI Dashboard

Connect Power BI Desktop to Snowflake via `Get Data → Snowflake`:
- **Server:** `your_account.snowflakecomputing.com`
- **Database:** `FLIGHT_ANALYSIS`
- **Schema:** `STAGING`
- **Mode:** DirectQuery (live data) or Import

### Recommended Visuals

| **Visual** | **Fields** | **Business Question** |
|---|---|---|
| **4 KPI Cards** | total_flights, avg_on_time_pct, avg_arr_delay, cancel_rate | Overall health snapshot |
| **Line Chart** | year_month (X), avg_arr_delay + on_time_pct (Y) | How has delay trended? |
| **Bar Chart** | airline_name (Y), avg_on_time_pct (X) | Which airline is best? |
| **Map** | lat/lon (size = departures, colour = on_time_pct) | Which airports have worst delays? |
| **Stacked Bar** | airline_code, carrier/weather/nas/late_aircraft % | What causes delays? |
| **Matrix Heat Map** | airline (rows), month (columns), on_time_pct (values) | Month-over-month trends |

---

## 📐 Data Quality Framework

> **The Golden Rule: Bronze = Observe. Silver = Validate. Gold = Trust.**
> Never discard data at Bronze — flag it, count it, and let downstream layers decide.

### Bronze — Observation Only (additive DQ flags)

| **Check** | **Column(s)** | **Expected Rate** |
|---|---|---|
| `FlightDate_null` | FlightDate IS NULL | < 0.001% |
| `Reporting_Airline_null` | Airline IS NULL | < 0.001% |
| `Origin_null` / `Dest_null` | Either IS NULL | < 0.001% |
| `dep_delay_flag` | DepDelay NOT BETWEEN -120 and 1440 | < 0.1% |
| `is_domestic_flag` | Distance NOT BETWEEN 1 and 10000 | < 0.01% |
| `same_origin_destination` | Origin == Dest | < 0.001% |
| `_corrupt_record` | Row failed CSV parse | < 0.01% |

### Silver — Validation & Enrichment

| **Check** | **Rule** |
|---|---|
| `total_null_count` | Sum of all 7 Bronze flags |
| `is_data_clean` | `VALID` if score ≤ 2, else `INVALID` |
| Deduplication | Window `ROW_NUMBER` on natural key, keep row 1 |
| Delay cause mismatch | Sum of cause delays must be within ±5 min of `ArrDelay` |
| Unknown airport | Origin/Dest IATA not found in airport reference |
| Haversine validation | BTS distance must be within 15% of great-circle distance |

### Gold — Aggregate Sanity

- On-time % must be between **0% and 100%** for every airline-month
- P95 delay ≥ Median delay for same airline-month
- Monthly rank values are **consecutive integers** with no gaps

---

## 📦 Expected Data Volumes

| **Layer** | **Table** | **Rows** | **Delta Size** |
|---|---|---|---|
| Bronze | `bronze_flights` | ~19–20 million | ~4.5 GB |
| Bronze | `bronze_airports` | ~900 (US only) | < 1 MB |
| Silver | `silver_flights` | ~18.5–19.5 million | ~3.8 GB |
| Gold | `monthly_airline_kpi` | ~500–600 | < 1 MB |
| Gold | `route_annual_performance` | ~15,000–20,000 | < 5 MB |
| Gold | `airport_monthly_performance` | ~10,000–15,000 | < 5 MB |
| Gold | `delay_cause_analysis` | ~500–600 | < 1 MB |

---

## 💰 Cost Summary

| **Platform** | **Tier** | **Estimated Cost** |
|---|---|---|
| Amazon S3 | Free tier | $0–$2/month |
| Databricks | Community Edition | $0 |
| Snowflake | 30-day free trial | $5–$15 total |
| Power BI Desktop | Free forever | $0 |
| **Total** | | **~$5–$20** |

---

## 🔧 Troubleshooting

<details>
<summary><b>S3 Access Denied in Databricks</b></summary>

- Verify `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set as **cluster environment variables**, not hardcoded in notebook cells
- Check that the S3 bucket region matches your `s3a.endpoint` setting
- Ensure the IAM user has `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on the bucket ARN

</details>

<details>
<summary><b>Databricks Cluster Terminates Mid-Job</b></summary>

- Community Edition clusters auto-terminate after **2 hours of inactivity** — this cannot be extended
- Solution: Split Bronze load into yearly batches (`2021 → 2022 → 2023` separately)
- Delta Lake is idempotent — re-running `mode("overwrite")` on a specific partition is always safe

</details>

<details>
<summary><b>Snowflake Connector ClassNotFoundException</b></summary>

- The Snowflake Maven library must be installed on the cluster **before** running any notebook cell
- Go to: Cluster → Libraries → Install New → Maven → enter coordinates:
  ```
  net.snowflake:spark-snowflake_2.12:2.12.0-spark_3.4
  net.snowflake:snowflake-jdbc:3.14.3
  ```
- **Restart the cluster** after adding libraries

</details>

<details>
<summary><b>BTS CSV Corrupt Records</b></summary>

- BTS CSVs end every row with a trailing comma — this creates an extra unnamed column
- The explicit schema handles this via the `_c109` / `_corrupt_record` fields
- Do **not** add a 37th field to the 36-column selection
- If corrupt record count > 0.1%, BTS may have changed their file format for that month

</details>

<details>
<summary><b>Snowflake Pipe Not Loading Data</b></summary>

- Run `ALTER PIPE pipe_name REFRESH;` to manually trigger ingestion
- Check `LIST @gold_stage_s3;` to confirm Parquet files are present in the stage
- Verify `MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE` — column names between Parquet and Snowflake table must match
- Confirm the `PATTERN = '.*part.*\.parquet'` matches your actual Spark output filenames

</details>

---

## 🔑 Key Concepts Used

| **Concept** | **Where Used** |
|---|---|
| `StructType` / `StructField` | Manual 110-column schema (Phase 1) |
| **PERMISSIVE read mode** | Capture corrupt rows without job failure (Phase 2) |
| **Delta Lake partitioning** | `partitionBy("year", "month")` for fast pruning (Phase 2, 3) |
| **DQ flag columns** | 7 additive flag columns on Bronze (Phase 2) |
| **Window functions (`ROW_NUMBER`)** | Deduplication on natural key (Phase 3) |
| **Broadcast join** | Small airport reference joined to large flight data (Phase 3) |
| **Haversine formula** | Cross-validate BTS distances with great-circle geometry (Phase 3) |
| **Custom UDF** | Extract flight number characters (Phase 3) |
| `percentile_approx` | Median delay calculation at scale (Phase 4) |
| **Spark caching / TempView** | Performance benchmarking (Phase 4) |
| **`RANK()` window function** | Monthly airline ranking (Phase 4) |
| **Snowflake Auto-Ingest Pipe** | S3 → Snowflake event-driven ingestion (Phase 5) |
| `dataSkippingNumIndexedCols` | Column indexing for faster Silver/Gold reads |

---

## 🚀 Project Highlights

- ✅ **50M+ records** processed end-to-end using PySpark on Databricks
- ✅ **Medallion Architecture** — clean separation of Bronze, Silver, and Gold layers
- ✅ **7 Data Quality flags** applied at Bronze — zero data loss philosophy
- ✅ **Broadcast join** used for airport enrichment — avoids unnecessary data shuffle
- ✅ **Haversine distance** calculated using PySpark built-in functions — no external libraries needed
- ✅ **Auto-ingest Snowpipes** — event-driven, zero-touch data loading from S3 to Snowflake
- ✅ **Data retention** set to 10 days on all Gold Snowflake tables — supports time travel queries
- ✅ **Cost-effective** — entire pipeline built using free/trial tier tools (~$5–$20 total)

---

<div align="center">

**Built as a hands-on Data Engineering learning project covering the complete modern Data Lakehouse stack.**

Data Source: [Bureau of Transportation Statistics (BTS) TranStats](https://www.transtats.bts.gov/)

---

*Created by **Krish Kumawat** — US Domestic Flight Delay Analytics Platform*

</div>
