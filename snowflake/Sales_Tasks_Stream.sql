use role dev_role;
use database retail_db;
use schema bronze_raw;

create or replace stream sales_transactions_raw_stream 
on table sales_transactions_raw;

create or replace stream sales_details_raw_stream
on table sales_details_raw;

use schema silver_clean;

create or replace task load_sales_silver 
    warehouse = COMPUTE_WH
    schedule = '5 MINUTE'
WHEN 
    SYSTEM$STREAM_HAS_DATA('RETAIL_DB.BRONZE_RAW.SALES_DETAILS_RAW_STREAM')
AS 
    MERGE INTO SILVER_CLEAN.FACT_SALE target 
USING (
    SELECT 
        details.RAW_DATA:transaction_id::INT AS transaction_id,
        details.RAW_DATA:product_id::INT AS product_id,
        trans.RAW_DATA:customer_id::INT AS customer_id,
        details.RAW_DATA:quantity::INT AS quantity,
        details.RAW_DATA:price_at_sale::NUMBER(10,2) AS price_at_sale,
        TO_TIMESTAMP_NTZ(trans.RAW_DATA:transaction_date::BIGINT / 1000000000) AS transaction_date
    FROM 
        BRONZE_RAW.SALES_DETAILS_RAW_STREAM details
    JOIN 
        BRONZE_RAW.SALES_TRANSACTIONS_RAW_STREAM trans 
    ON details.RAW_DATA:transaction_id = trans.RAW_DATA:transaction_id
) source 
ON target.transaction_id = source.transaction_id 
AND target.product_id = source.product_id
WHEN NOT MATCHED THEN 
    INSERT(transaction_id, product_id, customer_id, quantity, price_at_sale, transaction_date)
    VALUES (source.transaction_id, source.product_id, source.customer_id, source.quantity, source.price_at_sale, source.transaction_date);

alter task load_sales_silver resume;


    