create warehouse transforming;
create database raw;
create database analytics;
create schema raw.jaffle_shop;
create schema raw.stripe;

SELECT CURRENT_WAREHOUSE();
select * from information_schema.tables where created is not null;

create schema raw_schema; --im creating this schema as a output to store from dbt


create table raw.jaffle_shop.customers 
( id integer,
  first_name varchar,
  last_name varchar
);

copy into raw.jaffle_shop.customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    ); 


copy into raw.jaffle_shop.customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    ); 

create table raw.jaffle_shop.orders
( id integer,
  user_id integer,
  order_date date,
  status varchar,
  _etl_loaded_at timestamp default current_timestamp
);

copy into raw.jaffle_shop.orders (id, user_id, order_date, status)
from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );

create table raw.stripe.payment 
( id integer,
  orderid integer,
  paymentmethod varchar,
  status varchar,
  amount integer,
  created date,
  _batched_at timestamp default current_timestamp
);

copy into raw.stripe.payment (id, orderid, paymentmethod, status, amount, created)
from 's3://dbt-tutorial-public/stripe_payments.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );

select * from raw.jaffle_shop.customers;
select * from raw.jaffle_shop.orders;
select * from raw.stripe.payment; 


show views;
select * from information_schema.tables;
show tables;

select * from CUSTOMERS;

show views;
select * from customers;

show databases;
use database raw;
use schema jaffle_shop;
select * from customers;
select * from orders;
desc table orders;

select * from analytics.raw_schema.customers; 
desc table analytics.raw_schema.customers;

select * from result;

create table result as
with customer as (
    select id as customer_id, first_name, last_name from customers
),
orderss as (
    select
    id as order_id,
    user_id as customer_id, order_date, status from orders
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders
    from orderss
    group by 1
),

final as (
    select
        customer.customer_id,
        customer.first_name,
        customer.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders
    from customer
    left join customer_orders 
	on customer.customer_id = customer_orders.customer_id
)
select * from final;

select * from customers;
select * from orders;

with customers as (
    select id as customer_id, first_name, last_name from raw.jaffle_shop.customers
),
orders as (
    select id as order_id, user_id as customer_id, order_date, status from raw.jaffle_shop.orders
),
customer_orders as (
    select  customer_id, min(order_date) as first_order_date, max(order_date)    most_recent_order_date, count(*) number_of_orders from orders
group by customer_id    
),

final as (
    select customers.customer_id, customers.first_name, customers.last_name,
    customer_orders.first_order_date, customer_orders.most_recent_order_date, 
    coalesce(customer_orders.number_of_orders,0) as number_of_orders
    from customers
    left join customer_orders
    on customers.customer_id = customer_orders.customer_id
)
select * from final;


WITH customers AS (
    SELECT id AS customer_id, first_name, last_name 
    FROM raw.jaffle_shop.customers
),
orders AS (
    SELECT id AS order_id, user_id AS customer_id, order_date, status 
    FROM raw.jaffle_shop.orders
),
customer_orders AS (
    SELECT customer_id, 
           MIN(order_date) AS first_order_date, 
           MAX(order_date) AS most_recent_order_date, 
           COUNT(order_id) AS number_of_orders 
    FROM orders
    GROUP BY 1
),
final AS (
    SELECT customers.customer_id, 
           customers.first_name, 
           customers.last_name,
           customer_orders.first_order_date, 
           customer_orders.most_recent_order_date, 
           COALESCE(customer_orders.number_of_orders, 0) AS number_of_orders
    FROM customers
    LEFT JOIN customer_orders
    ON customers.customer_id = customer_orders.customer_id
)
SELECT * FROM final;

SELECT CURRENT_ROLE();


select * from information_schema.tables where created is not null;

select * from dim_customer;


select current_account();
select current_user();

https://hv97788.ap-southeast-1.snowflakecomputing.com
;

select * from information_schema.tables where created is not null;

select * from STG_JAFFLE_SHOP__CUSTOMERS;
select distinct status from STG_JAFFLE_SHOP__ORDERS;

select * from RAW.VIJAY_SCHEMA.DIM_CUSTOMER;
select  max(_ETL_LOADED_AT) as max_loaded_at,
      convert_timezone('UTC', current_timestamp()) as snapshotted_at from RAW.jaffle_shop.orders;













