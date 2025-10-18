use role dev_role;
use database retail_db;
use schema bronze_raw;


create or replace stream product_raw_stream 
on table products_raw;

use schema silver_clean;

create or replace task load_products_silver
    warehouse = COMPUTE_WH
    schedule = '5 MINUTE'
when SYSTEM$STREAM_HAS_DATA('RETAIL_DB.BRONZE_RAW.PRODUCT_RAW_STREAM')
as 
    MERGE INTO SILVER_CLEAN.DIM_PRODUCT target 
USING (
    select 
        value:product_id::INT as product_id,
        value:product_name::STRING as product_name,
        value:category::STRING as category,
        value:selling_price::NUMBER(10,2) as selling_price,
        value:cost_price::NUMBER(10,2) as cost_price
    from 
        bronze_raw.product_raw_stream,
        LATERAL FLATTEN(input => RAW_DATA)
) source 
on target.product_id = source.product_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.product_name = source.product_name,
        target.category = source.category,
        target.selling_price = source.selling_price,
        target.cost_price = source.cost_price
WHEN NOT MATCHED THEN 
    INSERT (product_id, product_name, category, selling_price, cost_price)
    VALUES (source.product_id, source.product_name, source.category, source.selling_price, source.cost_price);


ALTER TASK LOAD_PRODUCTS_SILVER RESUME;