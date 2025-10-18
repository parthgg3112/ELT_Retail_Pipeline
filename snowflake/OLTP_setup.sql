---- creating raw bronze layer table for sales to handle json, parquet formatted data
create or replace table sales_transactions_raw (
    raw_data variant,
    metadata_filename varchar,
    load_timestamp timestamp_ntz default current_timestamp()
);

use database retail_db;

---- creating file format : parquet
create or replace file format parquet_format
type = 'PARQUET';


---- creating storage integration to connect Snowflake to S3
create or replace storage integration s3_integration
    type = EXTERNAL_STAGE
    storage_provider = 'S3'
    enabled = TRUE
    STORAGE_AWS_ROLE_ARN = <STORAGE_AWS_ROLE_ARN>
    storage_allowed_locations = <storage_allowed_locations>

desc integration s3_integration;

GRANT USAGE ON SCHEMA RETAIL_DB.BRONZE_RAW TO ROLE DEV_ROLE;
GRANT CREATE TABLE ON SCHEMA RETAIL_DB.BRONZE_RAW TO ROLE DEV_ROLE;

use role dev_role;

---- creating stage and pipe for the data to be loaded in the table.
create or replace stage s3_sales_transactions_stage
    URL = <s3_URL>
    storage_integration = s3_integration
    file_format = parquet_format;

use role ACCOUNTADMIN;

use schema retail_db.bronze_raw;

create or replace pipe sales_transactions_snowpipe 
    auto_ingest = TRUE 
as 
 copy into sales_transactions_raw(raw_data, metadata_filename)
 from (
    select $1, METADATA$FILENAME from @s3_sales_transactions_stage
);

show pipes;

create or replace table sales_details_raw (
    raw_data variant,
    metadata_filename varchar,
    load_timestamp timestamp_ntz default current_timestamp()
);

create or replace stage s3_sales_details_stage
    URL = <s3_URL>
    storage_integration = s3_integration
    file_format = parquet_format;

show stages;

create or replace pipe sales_details_snowpipe
    auto_ingest = TRUE
as 
    copy into sales_details_raw(raw_data,metadata_filename)
from (
    select $1, METADATA$FILENAME from @s3_sales_details_stage
);

show pipes;


