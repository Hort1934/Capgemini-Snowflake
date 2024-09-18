-- Step 1: Create a Database and Warehouse
CREATE DATABASE IF NOT EXISTS nyc_airbnb;
USE DATABASE nyc_airbnb;

CREATE WAREHOUSE IF NOT EXISTS nyc_airbnb_wh WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- Step 2: Create a Stage for the raw data in your cloud storage (e.g., AWS S3)
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://my-bucket-name/my_path-to-file/'
CREDENTIALS = (AWS_KEY_ID = 'my-key' AWS_SECRET_KEY = 'my-secret-key');

-- Step 3: Create a table to hold the raw Airbnb data
CREATE OR REPLACE TABLE raw_airbnb_data (
    id INT,
    name STRING,
    host_id INT,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude FLOAT,
    longitude FLOAT,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
);

-- Step 4: Create a Snowpipe for continuous data ingestion
CREATE OR REPLACE PIPE my_pipe
  AUTO_INGEST = TRUE
  AS
  COPY INTO raw_airbnb_data
  FROM @my_s3_stage
  FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);

-- Step 5: Create a table for transformed Airbnb data
CREATE OR REPLACE TABLE airbnb_transformed (
    id INT,
    name STRING,
    host_id INT,
    host_name STRING,
    neighbourhood_group STRING,
    neighbourhood STRING,
    latitude FLOAT,
    longitude FLOAT,
    room_type STRING,
    price INT,
    minimum_nights INT,
    number_of_reviews INT,
    last_review DATE,
    reviews_per_month FLOAT,
    calculated_host_listings_count INT,
    availability_365 INT
);

-- Step 6: Transformation logic: Cleaning and transforming data
INSERT INTO airbnb_transformed
SELECT
    id,
    name,
    host_id,
    host_name,
    neighbourhood_group,
    neighbourhood,
    latitude,
    longitude,
    room_type,
    price,
    minimum_nights,
    number_of_reviews,
    -- Handling last_review date and filling missing values
    COALESCE(TRY_TO_DATE(last_review), (SELECT MIN(last_review) FROM raw_airbnb_data)) AS last_review,
    -- Handling missing reviews_per_month values
    COALESCE(reviews_per_month, 0) AS reviews_per_month,
    calculated_host_listings_count,
    availability_365
FROM raw_airbnb_data
WHERE price > 0
AND latitude IS NOT NULL
AND longitude IS NOT NULL;

-- Step 7: Set up a Stream to track changes in the raw table
CREATE OR REPLACE STREAM raw_airbnb_stream ON TABLE raw_airbnb_data;

-- Step 8: Create a Task to run daily transformations and loading
CREATE OR REPLACE TASK transform_task
  WAREHOUSE = nyc_airbnb_wh
  SCHEDULE = 'USING CRON 0 0 * * * UTC'  -- Runs daily at midnight
AS
  INSERT INTO airbnb_transformed
  SELECT
      id,
      name,
      host_id,
      host_name,
      neighbourhood_group,
      neighbourhood,
      latitude,
      longitude,
      room_type,
      price,
      minimum_nights,
      number_of_reviews,
      COALESCE(TRY_TO_DATE(last_review), (SELECT MIN(last_review) FROM raw_airbnb_data)) AS last_review,
      COALESCE(reviews_per_month, 0) AS reviews_per_month,
      calculated_host_listings_count,
      availability_365
  FROM raw_airbnb_data
  WHERE price > 0
  AND latitude IS NOT NULL
  AND longitude IS NOT NULL;

-- Step 9: Monitor using Time Travel
-- Example: Recover historical data
SELECT * FROM airbnb_transformed AT (OFFSET => -1);


