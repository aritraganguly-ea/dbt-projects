-- Snowflake User Creation.

-- Step 1: Use an admin role.
USE ROLE ACCOUNTADMIN;

-- Step 2: Create the `transform` role and assign it to ACCOUNTADMIN.
CREATE ROLE IF NOT EXISTS TRANSFORM;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Step 3: Create a default warehouse.
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Step 4: Create the `dbt` user and assign it to the transform role.
CREATE USER IF NOT EXISTS dbt
  PASSWORD='dbtPassword123'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE=TRANSFORM
  DEFAULT_NAMESPACE='MOVIELENS.RAW'
  COMMENT='MovieLens 20M Data Transformation';
ALTER USER dbt SET TYPE = LEGACY_SERVICE;
GRANT ROLE TRANSFORM TO USER dbt;

-- Step 5: Create a database and schema for the MovieLens project.
CREATE DATABASE IF NOT EXISTS MOVIELENS;
CREATE SCHEMA IF NOT EXISTS MOVIELENS.RAW;

-- Step 6: Grant permissions to the `transform` role.
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;
GRANT ALL ON DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE MOVIELENS TO ROLE TRANSFORM;
GRANT ALL ON ALL TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;
GRANT ALL ON FUTURE TABLES IN SCHEMA MOVIELENS.RAW TO ROLE TRANSFORM;


-- Create an external stage named 'movielens'
CREATE STAGE movielens
  URL='s3://netflix-movielens-20m'
  CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');

  
-- Load data from AWS S3 bucket into the raw_movies table.
CREATE OR REPLACE TABLE raw_movies (
  movieId INTEGER,
  title STRING,
  genres STRING
);

COPY INTO raw_movies
FROM @movielens/movies.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load data from AWS S3 bucket into the raw_ratings table.
CREATE OR REPLACE TABLE raw_ratings (
  userId INTEGER,
  movieId INTEGER,
  rating FLOAT,
  timestamp BIGINT
);

COPY INTO raw_ratings
FROM @movielens/ratings.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load data from AWS S3 bucket into the raw_tags table.
CREATE OR REPLACE TABLE raw_tags (
  userId INTEGER,
  movieId INTEGER,
  tag STRING,
  timestamp BIGINT
);

COPY INTO raw_tags
FROM @movielens/tags.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

-- Load data from AWS S3 bucket into the raw_genome_scores table.
CREATE OR REPLACE TABLE raw_genome_scores (
  movieId INTEGER,
  tagId INTEGER,
  relevance FLOAT
);

COPY INTO raw_genome_scores
FROM @movielens/genome-scores.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load data from AWS S3 bucket into the raw_genome_tags table.
CREATE OR REPLACE TABLE raw_genome_tags (
  tagId INTEGER,
  tag STRING
);

COPY INTO raw_genome_tags
FROM @movielens/genome-tags.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');

-- Load data from AWS S3 bucket into the raw_links table.
CREATE OR REPLACE TABLE raw_links (
  movieId INTEGER,
  imdbId INTEGER,
  tmdbId INTEGER
);

COPY INTO raw_links
FROM @movielens/links.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');