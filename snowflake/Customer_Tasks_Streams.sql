use role dev_role;
use database retail_db;
use schema bronze_raw;


create or replace stream customer_raw_stream
on table customers_raw;

use schema silver_clean;

create or replace task load_customer_silver
    warehouse = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
when 
    SYSTEM$STREAM_HAS_DATA('BRONZE_RAW.CUSTOMER_RAW_STREAM')
as 
    MERGE INTO SILVER_CLEAN.DIM_CUSTOMER target 
using (
    select 
        RAW_DATA:customer_id::INT as customer_id,
        RAW_DATA:first_name::STRING as first_name,
        RAW_DATA:last_name::STRING as last_name,
        RAW_DATA:email::STRING as email,
        RAW_DATA:street_address::STRING as street_address,
        RAW_DATA:city::STRING as city,
        RAW_DATA:state::STRING as state,
        RAW_DATA:registration_date::DATE as registration_date
    from 
        BRONZE_RAW.CUSTOMER_RAW_STREAM
) source 
ON target.customer_id = source.customer_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.first_name = source.first_name,
        target.last_name = source.last_name,
        target.email = source.email,
        target.street_address = source.street_address,
        target.city = source.city,
        target.state = source.state,
        target.registration_date = source.registration_date
WHEN NOT MATCHED THEN 
    INSERT (customer_id, first_name, last_name, email, street_address, city, state, registration_date)
    VALUES (source.customer_id, source.first_name, source.last_name, source.email, source.street_address, source.city, source.state, source.registration_date);


alter task load_customer_silver resume;