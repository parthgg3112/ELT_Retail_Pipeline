---- for creating roles, initial setup
use role ACCOUNTADMIN;

--- creating a role specific to the project rather than using ADMIN roles
create role if not exists DEV_ROLE;
grant role DEV_ROLE to user parthgg31;

---- creating warehouse for the project
create warehouse if not exists RETAIL_COMPUTE_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

--- granting permissions
GRANT USAGE ON WAREHOUSE RETAIL_COMPUTE_WH TO ROLE DEV_ROLE;

-- creating database for the project
create database if not exists retail_db;
grant ownership on database retail_db to role dev_role;

use role dev_role;
use warehouse retail_compute_wh;
use database retail_db;

--- creating schemas for the project
create schema if not exists retail_db.bronze_raw;
create schema if not exists retail_db.silver_clean;
create schema if not exists retail_db.gold_analytics;