/* ======================================================================
   Purpose: Create automation (Stream, SP, Task) to process 
            raw JSON events into structured tables.
   Role Required: SWIGGY_TASK_RUNNER
   ====================================================================== */

-- ----------------------------------------------------------------------
-- Switch to TASK RUNNER ROLE
-- ----------------------------------------------------------------------
USE ROLE SWIGGY_TASK_RUNNER;

-- ----------------------------------------------------------------------
-- Step 1: Create Stream
-- Tracks new rows inserted into the EVENTS_RAW landing table.
-- ----------------------------------------------------------------------
USE SCHEMA SWIGGY_DB.RAW;
CREATE OR REPLACE STREAM EVENTS_RAW_STREAM 
ON TABLE SWIGGY_DB.RAW.EVENTS_RAW
APPEND_ONLY = TRUE; 


-- ----------------------------------------------------------------------
-- Step 2: Create a Stored Procedure
-- Contains the 10 INSERT statements to flatten JSON from the stream into the final tables.
-- ----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE SP_PROCESS_RAW_EVENTS()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  -- 1. CUSTOMER
  INSERT INTO SWIGGY_DB.RAW.CUSTOMER (
      customer_id, name, mobile, email, loginbyusing, gender, dob, preferences, created_date
  )
  SELECT COALESCE(payload:customer_id::string, UUID_STRING()),
         payload:name::string, payload:mobile::string, payload:email::string,
         payload:loginbyusing::string, payload:gender::string, TRY_TO_DATE(payload:dob::string),
         payload:preferences, TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'customer' AND METADATA$ACTION = 'INSERT';

  -- 2. CUSTOMER_ADDRESS
  INSERT INTO SWIGGY_DB.RAW.CUSTOMER_ADDRESS (
      address_id, customer_id, flatno, houseno, floor, building, landmark,
      coordinates, primaryflag, address_type, locality, city, state, pincode, created_date
  )
  SELECT COALESCE(payload:address_id::string, UUID_STRING()),
         payload:customer_id::string, payload:flatno::string, payload:houseno::string,
         payload:floor::string, payload:building::string, payload:landmark::string,
         payload:coordinates::string, payload:primaryflag::string, payload:address_type::string,
         payload:locality::string, payload:city::string, payload:state::string,
         payload:pincode::string, TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'customer_address' AND METADATA$ACTION = 'INSERT';

  -- 3. LOCATION
  INSERT INTO SWIGGY_DB.RAW.LOCATION (
      location_id, city, state, zipcode, activeflag, created_date
  )
  SELECT COALESCE(payload:location_id::string, UUID_STRING()),
         payload:city::string, payload:state::string, TRY_TO_NUMBER(payload:zipcode::string),
         payload:activeflag::string, TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'location' AND METADATA$ACTION = 'INSERT';

  -- 4. RESTAURANT
  INSERT INTO SWIGGY_DB.RAW.RESTAURANT (
      restaurant_id, name, cuisine_type, pricing_for_2, location_id, created_date
  )
  SELECT COALESCE(payload:restaurant_id::string, UUID_STRING()),
         payload:name::string, payload:cuisine_type::string,
         TRY_TO_NUMBER(payload:pricing_for_2::string), payload:location_id::string,
         TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'restaurant' AND METADATA$ACTION = 'INSERT';

  -- 5. MENU
  INSERT INTO SWIGGY_DB.RAW.MENU (
      menu_id, restaurant_id, itemname, description, price, activeflag, created_date
  )
  SELECT COALESCE(payload:menu_id::string, UUID_STRING()),
         payload:restaurant_id::string, payload:itemname::string, payload:description::string,
         TRY_TO_NUMBER(payload:price::string), payload:activeflag::string,
         TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'menu' AND METADATA$ACTION = 'INSERT';

  -- 6. ORDERS
  INSERT INTO SWIGGY_DB.RAW.ORDERS (
      order_id, customer_id, restaurant_id, order_date, totalamount, status, paymentmethod, created_date
  )
  SELECT COALESCE(payload:order_id::string, UUID_STRING()),
         payload:customer_id::string, payload:restaurant_id::string,
         TRY_TO_TIMESTAMP_TZ(payload:order_date::string), TRY_TO_NUMBER(payload:totalamount::string),
         payload:status::string, payload:paymentmethod::string,
         TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'orders' AND METADATA$ACTION = 'INSERT';

  -- 7. ORDER_ITEMS
  INSERT INTO SWIGGY_DB.RAW.ORDER_ITEMS (
      orderitem_id, order_id, menu_id, quantity, price, subtotal
  )
  SELECT COALESCE(payload:orderitem_id::string, UUID_STRING()),
         payload:order_id::string, payload:menu_id::string, TRY_TO_NUMBER(payload:quantity::string),
         TRY_TO_NUMBER(payload:price::string), TRY_TO_NUMBER(payload:subtotal::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'orderitem' AND METADATA$ACTION = 'INSERT';

  -- 8. DELIVERY_AGENTS
  INSERT INTO SWIGGY_DB.RAW.DELIVERY_AGENTS (
      deliveryagent_id, name, phone, vehicle_type, location_id, status, rating, created_date
  )
  SELECT COALESCE(payload:deliveryagent_id::string, UUID_STRING()),
         payload:name::string, payload:phone::string, payload:vehicle_type::string,
         payload:location_id::string, payload:status::string, TRY_TO_NUMBER(payload:rating::string),
         TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'deliveryagent' AND METADATA$ACTION = 'INSERT';

  -- 9. DELIVERY
  INSERT INTO SWIGGY_DB.RAW.DELIVERY (
      delivery_id, order_id, deliveryagent_id, deliverystatus, estimated_time, address_id, delivery_date, created_date
  )
  SELECT COALESCE(payload:delivery_id::string, UUID_STRING()),
         payload:order_id::string, payload:deliveryagent_id::string, payload:deliverystatus::string,
         payload:estimated_time::string, payload:address_id::string,
         TRY_TO_TIMESTAMP_TZ(payload:delivery_date::string), TRY_TO_TIMESTAMP_TZ(payload:created_date::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'delivery' AND METADATA$ACTION = 'INSERT';

  -- 10. LOGINAUDIT
  INSERT INTO SWIGGY_DB.RAW.LOGINAUDIT (
      login_id, customer_id, logintype, deviceinterface, mobiledevicename, webinterface, lastlogin
  )
  SELECT COALESCE(payload:login_id::string, UUID_STRING()),
         payload:customer_id::string, payload:logintype::string, payload:deviceinterface::string,
         payload:mobiledevicename::string, payload:webinterface::string,
         TRY_TO_TIMESTAMP_TZ(payload:lastlogin::string)
  FROM SWIGGY_DB.RAW.EVENTS_RAW_STREAM
  WHERE payload:type::string = 'loginaudit' AND METADATA$ACTION = 'INSERT';

  RETURN 'Successfully processed events.';

EXCEPTION
  WHEN OTHER THEN
    RETURN 'Failed: ' || SQLERRM;
END;
$$;


-- ----------------------------------------------------------------------
-- Step 3: Create Task
-- Runs the Stored Procedure every 1 minute, but only if the stream has new data.
-- ----------------------------------------------------------------------
USE SCHEMA SWIGGY_DB.RAW;
CREATE OR REPLACE TASK TASK_PROCESS_RAW_EVENTS
  WAREHOUSE = SWIGGY_WH
  SCHEDULE = '1 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('SWIGGY_DB.RAW.EVENTS_RAW_STREAM')
AS
  CALL SWIGGY_DB.RAW.SP_PROCESS_RAW_EVENTS();


-- ----------------------------------------------------------------------
-- Step 4: Activate the Task (it's created suspended)
-- ----------------------------------------------------------------------
ALTER TASK SWIGGY_DB.RAW.TASK_PROCESS_RAW_EVENTS RESUME;

ALTER PIPE SWIGGY_DB.RAW.SWIGGY_RAW_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE SWIGGY_DB.RAW.SWIGGY_RAW_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;