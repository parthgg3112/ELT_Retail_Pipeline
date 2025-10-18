use database retail_db;
use schema silver_clean;
use role dev_role;

create or replace table dim_customer (
customer_id int primary key,
first_name varchar,
last_name varchar,
email varchar,
street_address varchar,
city varchar,
state varchar,
registration_date date 
);

create or replace table dim_product(
product_id int primary key,
product_name varchar,
category varchar,
selling_price number(10,2),
cost_price number(10,2)
);

create or replace table fact_sale(
transaction_id int,
product_id int,
customer_id int,
quantity int,
price_at_sale number(10,2),
transaction_date timestamp_ntz,
constraint fk_product_id foreign key (product_id) references dim_product(product_id),
constraint fk_customer_id foreign key (customer_id) references dim_customer(customer_id)
);

