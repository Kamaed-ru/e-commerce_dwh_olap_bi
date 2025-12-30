CREATE DATABASE IF NOT EXISTS bi_analytics;
USE bi_analytics;

CREATE TABLE customers_mt
(
    customer_id UInt32,
    first_name Nullable(String),
    last_name Nullable(String),
    email Nullable(String),
    city_id Nullable(UInt32),
    age Nullable(UInt32),
    gender LowCardinality(String),
    registration_date Nullable(Date),
    updated_at Nullable(Date),
    deleted Nullable(UInt8),
    valid_from Nullable(Date),
    valid_to Date,
    is_current Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY (customer_id);

CREATE TABLE products_mt
(
    product_id UInt32,
    product_name Nullable(String),
    category_id Nullable(UInt32),
    price Nullable(Decimal(10,2)),
    weight Nullable(Decimal(10,2)),
    updated_at Nullable(Date),
    deleted Nullable(UInt8),
    valid_from Nullable(Date),
    valid_to Date,
    is_current Nullable(UInt8)
)
ENGINE = MergeTree()
ORDER BY product_id;

CREATE TABLE daiy_sales (
sales_date		Date32,
city_id			Int32,
product_id		Int32,
orders_count	Nullable(Int64),
total_quantity	Nullable(Int64),
total_revenue	Nullable(Decimal(18, 2)),
avg_order_value	Nullable(Decimal(18, 2)),
load_date		Nullable(Date32)
)
ENGINE = MergeTree()
ORDER BY (sales_date, city_id, product_id);


CREATE TABLE customer_ltv (
customer_id		Int32,
first_order		Nullable(Date32),
last_order		Nullable(Date32),
lifetime_days	Nullable(Int32),
orders_count	Nullable(Int64),
total_spent		Nullable(Decimal(18, 2)),
avg_check		Nullable(Decimal(18, 2)),
churn_risk		Nullable(Bool),
load_date		Date32
)
ENGINE = MergeTree()
ORDER BY (load_date, customer_id);

CREATE TABLE product_popularity (
product_id		Int32,
category_id		Int32,
orders_count	Nullable(Int64),
total_quantity	Nullable(Int64),
popularity_rank	Nullable(Int32),
revenue_share	Nullable(Decimal(5, 2)),
load_date		Date32
)
ENGINE = MergeTree()
ORDER BY (load_date, category_id, product_id);

CREATE TABLE retention_metrics (
cohort_month	Date32,
active_month	Nullable(Date32),
customers_count	Nullable(Int64),
retained_count	Nullable(Int64),
retention_rate	Nullable(Decimal(5, 2)),
load_date		Date32
)
ENGINE = MergeTree()
ORDER BY (load_date, cohort_month);