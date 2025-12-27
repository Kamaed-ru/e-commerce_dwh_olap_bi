CREATE SCHEMA IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.cities (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(50) NOT NULL,
    country VARCHAR(20) NOT NULL,
    created_at DATE,
    deleted BOOLEAN DEFAULT FALSE
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS ods.categories (
    category_id INT PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    created_at DATE,
    deleted BOOLEAN DEFAULT FALSE
) DISTRIBUTED REPLICATED;


CREATE TABLE IF NOT EXISTS ods.customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    city_id INT NOT NULL,
    age INT,
    gender char(1),
    registration_date DATE,
    updated_at DATE,
    deleted BOOLEAN DEFAULT FALSE,
    ingestion_date DATE
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS ods.products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(80) NOT NULL,
    category_id INT NOT NULL,
    price DECIMAL(10,2),
    weight_kg FLOAT,
    updated_at DATE,
    deleted BOOLEAN DEFAULT FALSE,
    ingestion_date DATE
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS ods.orders (
    order_id BIGINT NOT NULL,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    order_status VARCHAR(12) NOT NULL,
    ingestion_date DATE NOT NULL
)
WITH (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 3)
DISTRIBUTED BY (order_id);

CREATE TABLE IF NOT EXISTS ods.order_lines (
    order_id BIGINT NOT NULL,
    order_line_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    ingestion_date DATE NOT NULL
)
WITH (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 3)
DISTRIBUTED BY (order_id);
