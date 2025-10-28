/* =================================================================
   1) Create a Virtual Warehouse
   - Warehouses in Snowflake provide compute resources.
   ================================================================= */
CREATE OR REPLACE WAREHOUSE SWIGGY_WH WITH 
    WAREHOUSE_SIZE     = 'XSMALL'
    WAREHOUSE_TYPE     = 'STANDARD'
    AUTO_SUSPEND       = 300
    AUTO_RESUME        = TRUE
    MIN_CLUSTER_COUNT  = 1
    MAX_CLUSTER_COUNT  = 1;


/* =================================================================
   2) Create Database and Schemas
   - SWIGGY_DB will hold all schemas and objects.
   - RAW schema: for raw ingested data.
   - STAGING schema: for cleaned/transformed intermediate data.
   ================================================================= */
CREATE DATABASE IF NOT EXISTS SWIGGY_DB;
USE DATABASE SWIGGY_DB;
CREATE SCHEMA IF NOT EXISTS SWIGGY_DB.RAW;
CREATE SCHEMA IF NOT EXISTS SWIGGY_DB.STAGING;


/* =================================================================
   3) Create Roles
   - Roles define access privileges.
   - SWIGGY_ADMIN: Owns the database and final tables, grants permissions.
   - SWIGGY_LOADER: Owns the data ingestion pipeline (Stage, Pipe, EVENTS_RAW table).
   - SWIGGY_TASK_RUNNER: Owns the data transformation pipeline (Stream, Task, Stored Procedure).
   ================================================================= */
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS SWIGGY_ADMIN;
CREATE ROLE IF NOT EXISTS SWIGGY_LOADER;
CREATE ROLE IF NOT EXISTS SWIGGY_TASK_RUNNER;


/* =================================================================
   4) Create Users with Extended Attributes
   - LOGIN_NAME: Explicit login identifier.
   - PASSWORD: Initial password for the user.
   - DEFAULT_ROLE: Role assigned at login.
   - DEFAULT_WAREHOUSE: Warehouse automatically used for queries.
   - DEFAULT_NAMESPACE: Default database.schema context.
   - MUST_CHANGE_PASSWORD: If TRUE, forces the user to reset their password at first login.
                           If FALSE, the password remains active until changed manually.
   - COMMENT: Helpful description for auditing/administration.
   ================================================================= */

-- Admin User
CREATE USER IF NOT EXISTS SWIGGY_ADMIN_USER
    LOGIN_NAME            = 'swiggy_admin'
    PASSWORD              = 'Adm!n_92Xy#Qw7Lp'
    DEFAULT_ROLE          = SWIGGY_ADMIN
    DEFAULT_WAREHOUSE     = 'SWIGGY_WH'
    DEFAULT_NAMESPACE     = 'SWIGGY_DB.RAW'
    MUST_CHANGE_PASSWORD  = FALSE
    COMMENT               = 'Swiggy Admin user with full privileges';

-- Loader User
CREATE USER IF NOT EXISTS SWIGGY_LOADER_USER
    LOGIN_NAME            = 'swiggy_loader'
    PASSWORD              = 'Lo@der_T8z!Km4Vr'
    DEFAULT_ROLE          = SWIGGY_LOADER
    DEFAULT_WAREHOUSE     = 'SWIGGY_WH'
    DEFAULT_NAMESPACE     = 'SWIGGY_DB.RAW'
    MUST_CHANGE_PASSWORD  = FALSE
    COMMENT               = 'Swiggy Loader user for data ingestion and staging';

-- Task Runner User
CREATE USER IF NOT EXISTS SWIGGY_TASK_USER
    LOGIN_NAME            = 'swiggy_task'
    PASSWORD              = 'T@skRun_55Yp#Nc8'
    DEFAULT_ROLE          = SWIGGY_TASK_RUNNER
    DEFAULT_WAREHOUSE     = 'SWIGGY_WH'
    DEFAULT_NAMESPACE     = 'SWIGGY_DB.STAGING'
    MUST_CHANGE_PASSWORD  = FALSE
    COMMENT               = 'Swiggy Task Runner user for scheduled tasks and automation';


/* =================================================================
   5) Assign Roles to Users
   ================================================================= */
GRANT ROLE SWIGGY_ADMIN, SWIGGY_LOADER, SWIGGY_TASK_RUNNER
    TO USER <your_login_name>;

GRANT ROLE SWIGGY_ADMIN TO USER SWIGGY_ADMIN_USER;
GRANT ROLE SWIGGY_LOADER TO USER SWIGGY_LOADER_USER;
GRANT ROLE SWIGGY_TASK_RUNNER TO USER SWIGGY_TASK_USER;


/* =================================================================
   6) Grant All Privileges (using SWIGGY_ADMIN as owner)
   - Admins get ALL privileges.
   - Loader/Task Runner get USAGE privileges only.
   ================================================================= */
USE ROLE SWIGGY_ADMIN;

-- Warehouse Privileges
GRANT ALL ON WAREHOUSE SWIGGY_WH TO ROLE SWIGGY_ADMIN;
GRANT USAGE ON WAREHOUSE SWIGGY_WH TO ROLE SWIGGY_LOADER;
GRANT USAGE ON WAREHOUSE SWIGGY_WH TO ROLE SWIGGY_TASK_RUNNER;
GRANT USAGE, OPERATE ON WAREHOUSE SWIGGY_WH TO ROLE SWIGGY_TASK_RUNNER;

-- Database Privileges
GRANT ALL ON DATABASE SWIGGY_DB TO ROLE SWIGGY_ADMIN;
GRANT USAGE ON DATABASE SWIGGY_DB TO ROLE SWIGGY_LOADER;
GRANT USAGE ON DATABASE SWIGGY_DB TO ROLE SWIGGY_TASK_RUNNER;

-- RAW Schema Privileges (Ingestion)
GRANT ALL ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_ADMIN;
GRANT USAGE ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_LOADER;
GRANT USAGE ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_TASK_RUNNER;

-- STAGING Schema Privileges (Automation)
GRANT ALL ON SCHEMA SWIGGY_DB.STAGING TO ROLE SWIGGY_ADMIN;
GRANT USAGE ON SCHEMA SWIGGY_DB.STAGING TO ROLE SWIGGY_TASK_RUNNER;

-- Privileges for SWIGGY_LOADER (Ingestion)
GRANT CREATE TABLE ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_LOADER;
GRANT CREATE FILE FORMAT ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_LOADER;
GRANT CREATE STAGE ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_LOADER;
GRANT CREATE PIPE ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_LOADER;

-- Privileges for SWIGGY_TASK_RUNNER (Transformation)
GRANT CREATE TABLE, CREATE STREAM, CREATE PROCEDURE, CREATE TASK ON SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_TASK_RUNNER;
GRANT CREATE TASK ON SCHEMA SWIGGY_DB.STAGING TO ROLE SWIGGY_TASK_RUNNER;
GRANT CREATE PROCEDURE ON SCHEMA SWIGGY_DB.STAGING TO ROLE SWIGGY_TASK_RUNNER;
GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_TASK_RUNNER;
GRANT SELECT, INSERT ON FUTURE TABLES IN SCHEMA SWIGGY_DB.RAW TO ROLE SWIGGY_TASK_RUNNER;


/* =================================================================
   7) Grant Global Task Privilege (Requires ACCOUNTADMIN)
   ================================================================= */
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SWIGGY_TASK_RUNNER;