/* ======================================================================
   Purpose: Setup S3 Storage Integration, Landing Table, Stage, 
            and Snowpipe for near real-time ingestion from AWS S3.
   Role Required: ACCOUNTADMIN (for integration) & SWIGGY_LOADER
   ====================================================================== */

-- ----------------------------------------------------------------------
-- 1. Create S3 Storage Integration (Requires ACCOUNTADMIN)
-- This links Snowflake to your S3 bucket using an IAM role.
-- ----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Create storage integration for S3.
CREATE OR REPLACE STORAGE INTEGRATION SWIGGY_S3_INTEGRATION
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::150540468213:role/swiggy-sf-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://swiggy-data-generation/raw/events/');

-- **ACTION REQUIRED:** Run this command and provide the 'STORAGE_AWS_IAM_USER_ARN'
-- and 'STORAGE_AWS_EXTERNAL_ID' to your AWS admin to update the IAM Role policy.
DESC INTEGRATION SWIGGY_S3_INTEGRATION;

-- Grant usage on integration to the SWIGGY_LOADER role.
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON INTEGRATION SWIGGY_S3_INTEGRATION TO ROLE SWIGGY_LOADER;

-- Update integration if IAM role ARN changes.
ALTER STORAGE INTEGRATION SWIGGY_S3_INTEGRATION
SET STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::150540468213:role/swiggy-sf-role';

-- Verify integration again after the alteration.
DESC INTEGRATION SWIGGY_S3_INTEGRATION;


-- ----------------------------------------------------------------------
-- 2. Switch Context to Loader Role
-- ----------------------------------------------------------------------
USE ROLE SWIGGY_LOADER;
USE DATABASE SWIGGY_DB;
USE SCHEMA RAW;


-- ----------------------------------------------------------------------
-- 3. Create File Format and Landing Table
-- The EVENTS_RAW table is the single, temporary destination for all incoming JSON data.
-- ----------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT swiggy_jsonl_format
  TYPE = 'JSON'
  STRIP_OUTER_ARRAY = FALSE
  COMPRESSION = 'AUTO';

CREATE OR REPLACE TABLE EVENTS_RAW (
  ingest_time TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),  -- ingestion timestamp
  source_file VARCHAR,                                   -- source file name/path
  payload VARIANT,                                       -- raw JSON payload
  ingestion_id STRING DEFAULT UUID_STRING()              -- unique ingestion ID
);


-- ----------------------------------------------------------------------
-- 4. Create External Stage
-- Points to the specific S3 folder using the Integration and File Format.
-- ----------------------------------------------------------------------
CREATE OR REPLACE STAGE SWIGGY_DB.RAW.SWIGGY_STAGE
    URL = 's3://swiggy-data-generation/raw/events/'
    STORAGE_INTEGRATION = SWIGGY_S3_INTEGRATION
    FILE_FORMAT = swiggy_jsonl_format;


-- ----------------------------------------------------------------------
-- 5. Create Snowpipe
-- Automatically ingests files from the S3 stage upon file arrival.
-- The pipe's ARN is used to configure S3 event notifications (SNS/SQS).
-- ----------------------------------------------------------------------
CREATE OR REPLACE PIPE SWIGGY_DB.RAW.SWIGGY_RAW_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO SWIGGY_DB.RAW.EVENTS_RAW (source_file, payload)
FROM (
    SELECT 
        METADATA$FILENAME,       -- capture source filename
        PARSE_JSON($1)           -- parse JSON content (which is the first column $1)
    FROM @SWIGGY_DB.RAW.SWIGGY_STAGE
)
FILE_FORMAT = (FORMAT_NAME = 'swiggy_jsonl_format')
ON_ERROR = 'CONTINUE';

-- **ACTION REQUIRED:** Get the pipe's ARN (notification_channel) to configure S3 notifications.
SHOW PIPES LIKE 'SWIGGY_RAW_PIPE';
SELECT SYSTEM$PIPE_STATUS('SWIGGY_RAW_PIPE');