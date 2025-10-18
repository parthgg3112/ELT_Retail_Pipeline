use role dev_role;
use database retail_db;
use schema gold_analytics;

create or replace view v_monthly_sales_by_category as 
select 
    p.category,
    date_trunc('MONTH',s.transaction_date) as sales_month,
    sum(s.quantity * s.price_at_sale) as total_revenue,
    count(distinct s.transaction_id) as number_of_transactions,
    sum(s.quantity) as total_items_sold
from 
    silver_clean.fact_sale s
join 
    silver_clean.dim_product p on s.product_id = p.product_id
group by all 
order by sales_month desc, total_revenue desc;


create or replace view v_customer_lifetime_value as 
select 
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    c.state,
    sum(s.quantity * s.price_at_sale) as lifetime_value,
    rank() over(order by lifetime_value desc) as customer_rank
from 
    silver_clean.fact_sale s
join
    silver_clean.dim_customer c on s.customer_id = c.customer_id
group by all;