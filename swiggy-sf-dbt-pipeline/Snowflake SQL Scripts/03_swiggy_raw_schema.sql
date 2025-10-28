/* ======================================================================
   Purpose: Create RAW schema tables for the Swiggy Data Model.
   Role Required: SWIGGY_ADMIN
   ====================================================================== */

-- ----------------------------------------------------------------------
-- Switch to the admin role and target schema.
-- ----------------------------------------------------------------------
USE ROLE SWIGGY_ADMIN;
USE DATABASE SWIGGY_DB;
USE SCHEMA RAW;

-- ----------------------------------------------------------------------
-- Customer Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER (
  customer_id         VARCHAR PRIMARY KEY,
  name                VARCHAR,
  mobile              VARCHAR,
  email               VARCHAR,
  loginbyusing        VARCHAR,
  gender              VARCHAR,
  dob                 DATE,
  preferences         VARIANT,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Customer Address Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE CUSTOMER_ADDRESS (
  address_id          VARCHAR PRIMARY KEY,
  customer_id         VARCHAR,
  flatno              VARCHAR,
  houseno             VARCHAR,
  floor               VARCHAR,
  building            VARCHAR,
  landmark            VARCHAR,
  coordinates         VARCHAR,
  primaryflag         VARCHAR,
  address_type        VARCHAR,
  locality            VARCHAR,
  city                VARCHAR,
  state               VARCHAR,
  pincode             VARCHAR,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Location Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE LOCATION (
  location_id         VARCHAR PRIMARY KEY,
  city                VARCHAR,
  state               VARCHAR,
  zipcode             NUMBER(10,2),
  activeflag          VARCHAR,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Restaurant Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE RESTAURANT (
  restaurant_id       VARCHAR PRIMARY KEY,
  name                VARCHAR,
  cuisine_type        VARCHAR,
  pricing_for_2       NUMBER(10,2),
  location_id         VARCHAR,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Menu Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE MENU (
  menu_id             VARCHAR PRIMARY KEY,
  restaurant_id       VARCHAR,
  itemname            VARCHAR,
  description         VARCHAR,
  price               NUMBER(10,2),
  activeflag          VARCHAR,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Orders Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE ORDERS (
  order_id            VARCHAR PRIMARY KEY,
  customer_id         VARCHAR,
  restaurant_id       VARCHAR,
  order_date          TIMESTAMP_TZ,
  totalamount         NUMBER(10,2),
  status              VARCHAR,
  paymentmethod       VARCHAR,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Order Items Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE ORDER_ITEMS (
  orderitem_id        VARCHAR PRIMARY KEY,
  order_id            VARCHAR,
  menu_id             VARCHAR,
  quantity            INTEGER,
  price               NUMBER(10,2),
  subtotal            NUMBER(10,2),
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Delivery Agents Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE DELIVERY_AGENTS (
  deliveryagent_id    VARCHAR PRIMARY KEY,
  name                VARCHAR,
  phone               VARCHAR,
  vehicle_type        VARCHAR,
  location_id         VARCHAR,
  status              VARCHAR,
  rating              NUMBER(2,1),
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Delivery Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE DELIVERY (
  delivery_id         VARCHAR PRIMARY KEY,
  order_id            VARCHAR,
  deliveryagent_id    VARCHAR,
  deliverystatus      VARCHAR,
  estimated_time      VARCHAR,
  address_id          VARCHAR,
  delivery_date       TIMESTAMP_TZ,
  created_date        TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

-- ----------------------------------------------------------------------
-- Login Audit Table
-- ----------------------------------------------------------------------
CREATE OR REPLACE TABLE LOGINAUDIT (
  login_id            VARCHAR PRIMARY KEY,
  customer_id         VARCHAR,
  logintype           VARCHAR,
  deviceinterface     VARCHAR,
  mobiledevicename    VARCHAR,
  webinterface        VARCHAR,
  lastlogin           TIMESTAMP_TZ,
  ingested_at         TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);