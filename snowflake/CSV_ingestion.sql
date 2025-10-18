use database retail_db;
use schema bronze_raw;
use role dev_role;

create or replace table customers_raw (
raw_data variant,
metadata_filename varchar,
load_timestamp timestamp_ntz default current_timestamp()
);

create or replace file format csv_format 
type = 'csv'
field_optionally_enclosed_by = '"'
skip_header = 1;

CREATE OR REPLACE STAGE INTERNAL_CUSTOMER_STAGE
    FILE_FORMAT = CSV_FORMAT;

COPY INTO CUSTOMERS_RAW (RAW_DATA, METADATA_FILENAME)
FROM (
    SELECT
        OBJECT_CONSTRUCT(
            'customer_id', $1,
            'first_name', $2,
            'last_name', $3,
            'email', $4,
            'street_address', $5,
            'city', $6,
            'state', $7,
            'registration_date', $8
        ),
        METADATA$FILENAME
    FROM @INTERNAL_CUSTOMER_STAGE
);


select * from customers_raw;

SELECT RAW_DATA:customer_id::INT, RAW_DATA:first_name::STRING
FROM RETAIL_DB.BRONZE_RAW.CUSTOMERS_RAW
LIMIT 10;
