use role dev_role;
use database retail_db;
use schema bronze_raw;

create or replace table products_raw (
raw_data variant,
metadata_filename varchar,
load_timestamp timestamp_ntz default current_timestamp()
);

create or replace file format json_format 
type = 'json';


create or replace stage s3_products_stage
    URL = <s3_URL>
    storage_integration = s3_integration
    file_format = json_format;

create or replace pipe products_snowpipe 
    auto_ingest = TRUE 
as 
copy into products_raw (raw_data, metadata_filename)
from (
    select $1, METADATA$FILENAME from @s3_products_stage
);

show pipes;

