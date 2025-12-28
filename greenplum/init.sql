-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- CORE Schema and Tables Initialization Script
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS core;

-- ============ LOOKUPS (Cities, Categories) ============

CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.cities (
    city_sk BIGSERIAL PRIMARY KEY,
    city_id INT NOT NULL,
    city_name VARCHAR(50),
    country VARCHAR(20),
    created_at DATE,
    deleted BOOLEAN
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS core.categories (
    category_sk BIGSERIAL PRIMARY KEY,
    category_id INT NOT NULL,
    category_name VARCHAR(100),
    created_at DATE,
    deleted BOOLEAN
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS core.customers (
    customer_sk BIGSERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(100),
    city_id INT,
    age INT,
    gender CHAR(1),
    registration_date DATE,
    updated_at DATE,
    deleted BOOLEAN,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS core.products (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(80),
    category_id INT,
    price DECIMAL(10,2),
    weight_kg FLOAT,
    updated_at DATE,
    deleted BOOLEAN,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
) DISTRIBUTED REPLICATED;

CREATE TABLE IF NOT EXISTS core.orders (
    order_sk BIGSERIAL,
    order_id BIGINT NOT NULL,
    customer_id INT NOT NULL,
    order_date DATE NOT NULL,
    order_status VARCHAR(12),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
)
WITH (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 3)
DISTRIBUTED BY (order_id);

CREATE TABLE IF NOT EXISTS core.order_lines (
    order_line_sk BIGSERIAL,
    order_id BIGINT NOT NULL,
    order_line_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT,
    price DECIMAL(10,2),
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
)
WITH (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 3)
DISTRIBUTED BY (order_id);


-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

-- 1. Cities (SCD1 lookup)
CREATE OR REPLACE FUNCTION core.fill_cities()
RETURNS VOID AS $$
BEGIN
    DROP EXTERNAL TABLE IF EXISTS core.cities_ext;

    CREATE EXTERNAL TABLE core.cities_ext (
        city_id INT,
        city_name TEXT,
        country TEXT,
        created_at DATE,
        deleted BOOLEAN
    )
    LOCATION ('pxf://ecommerce-data/stg/cities/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';

    IF NOT EXISTS (SELECT 1 FROM core.cities_ext LIMIT 1) THEN
        DROP EXTERNAL TABLE IF EXISTS core.cities_ext;
        RAISE EXCEPTION 'No data found in external table core.cities_ext';
    END IF;


	TRUNCATE TABLE core.cities;

    INSERT INTO core.cities (city_id, city_name, country, created_at, deleted)
    SELECT city_id, city_name, country, created_at, deleted
    FROM core.cities_ext;

    DROP EXTERNAL TABLE IF EXISTS core.cities_ext;
END;
$$ LANGUAGE plpgsql;

-- 2. Categories (SCD1 lookup)
CREATE OR REPLACE FUNCTION core.fill_categories()
RETURNS VOID AS $$
BEGIN
    DROP EXTERNAL TABLE IF EXISTS core.categories_ext;

    CREATE EXTERNAL TABLE core.categories_ext (
        category_id INT,
        category_name TEXT,
        created_at DATE,
        deleted BOOLEAN
    )
    LOCATION ('pxf://ecommerce-data/stg/categories/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';

    IF NOT EXISTS (SELECT 1 FROM core.categories_ext LIMIT 1) THEN
        DROP EXTERNAL TABLE IF EXISTS core.categories_ext;
        RAISE EXCEPTION 'No data found in external table core.categories_ext';
    END IF;

    TRUNCATE TABLE core.categories;

    INSERT INTO core.categories (category_id, category_name, created_at, deleted)
    SELECT category_id, category_name, created_at, deleted
    FROM core.categories_ext;
    
    DROP EXTERNAL TABLE IF EXISTS core.categories_ext;
END;
$$ LANGUAGE plpgsql;

--- 3. Customers (SCD2)
CREATE OR REPLACE FUNCTION core.fill_customers(p_date DATE)
RETURNS VOID AS $$
DECLARE
    locate TEXT;
BEGIN
    locate := FORMAT('pxf://ecommerce-data/stg/customers/ingestion_date=%s/*.parquet?PROFILE=s3:parquet&SERVER=minio', p_date);

    DROP EXTERNAL TABLE IF EXISTS core.customers_ext;

    EXECUTE FORMAT(
        'CREATE EXTERNAL TABLE core.customers_ext (
            customer_id INT,
            first_name VARCHAR(100),
            last_name VARCHAR(100),
            email VARCHAR(100),
            city_id INT,
            age INT,
            gender CHAR(1),
            registration_date DATE,
            updated_at DATE,
            deleted BOOLEAN
        )
        LOCATION (%L)
        FORMAT ''CUSTOM'' (formatter=''pxfwritable_import'')
        ENCODING ''UTF8''',
        locate
    );

    IF NOT EXISTS (SELECT 1 FROM core.customers_ext LIMIT 1) THEN
        DROP EXTERNAL TABLE IF EXISTS core.customers_ext;
        RAISE EXCEPTION '%', FORMAT('No data found in external table core.customers for date %s', p_date);
    END IF;

    UPDATE core.customers t
    SET valid_to = p_date - 1,
        is_current = FALSE,
        deleted = TRUE
    FROM core.customers_ext s
    WHERE t.customer_id = s.customer_id
        AND t.is_current = TRUE
        AND s.deleted = TRUE;

    UPDATE core.customers t
    SET valid_to = p_date - 1,
        is_current = FALSE
    FROM core.customers_ext s
    WHERE t.customer_id = s.customer_id
      AND t.is_current = TRUE
      AND s.deleted = FALSE
      AND (
        t.first_name <> s.first_name
        OR t.last_name <> s.last_name
        OR t.email <> s.email
        OR t.city_id <> s.city_id
        OR t.age <> s.age
        OR t.gender <> s.gender
        OR t.deleted <> s.deleted
      );

    INSERT INTO core.customers (
        customer_id, first_name, last_name, email, city_id, age, gender,
        registration_date, updated_at, deleted,
        valid_from, valid_to, is_current
    )
    SELECT
        s.customer_id, s.first_name, s.last_name, s.email, s.city_id, s.age, s.gender,
        s.registration_date, s.updated_at, s.deleted,
        p_date, '9999-12-31', TRUE
    FROM core.customers_ext s
    LEFT JOIN core.customers t
        ON t.customer_id = s.customer_id
       AND t.is_current = TRUE
    WHERE t.customer_id IS NULL
        AND s.deleted = FALSE;

    DROP EXTERNAL TABLE IF EXISTS core.customers_ext;

    RETURN;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------

-- 4. Products (SCD2)
CREATE OR REPLACE FUNCTION core.fill_products(p_date DATE)
RETURNS VOID AS $$
DECLARE
    locate TEXT;
BEGIN
    locate := FORMAT('pxf://ecommerce-data/stg/products/ingestion_date=%s/*.parquet?PROFILE=s3:parquet&SERVER=minio', p_date);

    DROP EXTERNAL TABLE IF EXISTS core.products_ext;

    EXECUTE FORMAT(
        'CREATE EXTERNAL TABLE core.products_ext (
            product_id INT,
            product_name VARCHAR(80),
            category_id INT,
            price DECIMAL(10,2),
            weight_kg FLOAT,
            updated_at DATE,
            deleted BOOLEAN
        )
        LOCATION (%L)
        FORMAT ''CUSTOM'' (formatter=''pxfwritable_import'')
        ENCODING ''UTF8''',
        locate
    );

    IF NOT EXISTS (SELECT 1 FROM core.products_ext LIMIT 1) THEN
        DROP EXTERNAL TABLE IF EXISTS core.products_ext;
        RAISE EXCEPTION '%', FORMAT('No data found in external table core.products for date %s', p_date);
    END IF;

    UPDATE core.products t
    SET valid_to = p_date - 1,
        is_current = FALSE,
        deleted = TRUE
    FROM core.products_ext s
    WHERE t.product_id = s.product_id
        AND t.is_current = TRUE
        AND s.deleted = TRUE;

    UPDATE core.products t
    SET valid_to = p_date - 1,
        is_current = FALSE
    FROM core.products_ext s
    WHERE t.product_id = s.product_id
      AND t.is_current = TRUE
      AND s.deleted = FALSE
      AND (
           t.product_name <> s.product_name
        OR t.category_id <> s.category_id
        OR t.price <> s.price
        OR t.weight_kg <> s.weight_kg
        OR t.deleted <> s.deleted
      );

    INSERT INTO core.products (
        product_id, product_name, category_id, price, weight_kg,
        updated_at, deleted,
        valid_from, valid_to, is_current
    )
    SELECT
        s.product_id, s.product_name, s.category_id, s.price, s.weight_kg,
        s.updated_at, s.deleted,
        p_date, '9999-12-31', TRUE
    FROM core.products_ext s
    LEFT JOIN core.products t
        ON t.product_id = s.product_id
       AND t.is_current = TRUE
    WHERE t.product_id IS NULL
        AND s.deleted = FALSE;

    DROP EXTERNAL TABLE IF EXISTS core.products_ext;

    RETURN;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------

-- 5. Orders (SCD2)
CREATE OR REPLACE FUNCTION core.fill_orders(p_date DATE)
RETURNS VOID AS $$
DECLARE
    locate TEXT;
BEGIN
    locate := FORMAT('pxf://ecommerce-data/stg/orders/ingestion_date=%s/*.parquet?PROFILE=s3:parquet&SERVER=minio', p_date);


    DROP EXTERNAL TABLE IF EXISTS core.orders_ext;

    EXECUTE FORMAT(
        'CREATE EXTERNAL TABLE core.orders_ext (
            order_id BIGINT,
            customer_id INT,
            order_date DATE,
            order_status TEXT
        )
        LOCATION (%L)
        FORMAT ''CUSTOM'' (formatter=''pxfwritable_import'')
        ENCODING ''UTF8''',
        locate
    );

    IF NOT EXISTS (SELECT 1 FROM core.orders_ext LIMIT 1) THEN
        DROP EXTERNAL TABLE IF EXISTS core.orders_ext;
        RAISE EXCEPTION '%', FORMAT('No data found in external table core.orders for date %s', p_date);
    END IF;

    UPDATE core.orders t
    SET valid_to = p_date - 1,
        is_current = FALSE
    FROM core.orders_ext s
    WHERE t.order_id = s.order_id
      AND t.is_current = TRUE
      AND (
           t.customer_id <> s.customer_id
        OR t.order_date <> s.order_date
        OR t.order_status <> s.order_status
      );

    INSERT INTO core.orders (
        order_id, customer_id, order_date, order_status, valid_from, valid_to, is_current
    )
    SELECT
        s.order_id, s.customer_id, s.order_date, s.order_status,p_date, '9999-12-31', TRUE
    FROM core.orders_ext s
    LEFT JOIN core.orders t
        ON t.order_id = s.order_id
       AND t.is_current = TRUE
    WHERE t.order_id IS NULL;

    DROP EXTERNAL TABLE IF EXISTS core.orders_ext;

    RETURN;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------

-- 6. Order Lines (SCD2)
CREATE OR REPLACE FUNCTION core.fill_order_lines(p_date DATE)
RETURNS VOID AS $$
DECLARE
    locate TEXT;
BEGIN
    locate := FORMAT('pxf://ecommerce-data/stg/order_lines/ingestion_date=%s/*.parquet?PROFILE=s3:parquet&SERVER=minio', p_date);

    DROP EXTERNAL TABLE IF EXISTS core.order_lines_ext;

    EXECUTE FORMAT(
        'CREATE EXTERNAL TABLE core.order_lines_ext (
            order_id BIGINT,
            order_line_id INT,
            product_id INT,
            quantity INT,
            price DECIMAL(10,2)
        )
        LOCATION (%L)
        FORMAT ''CUSTOM'' (formatter=''pxfwritable_import'')
        ENCODING ''UTF8''',
        locate
    );

    IF NOT EXISTS (SELECT 1 FROM core.order_lines_ext LIMIT 1) THEN
        DROP EXTERNAL TABLE IF EXISTS core.order_lines_ext;
        RAISE EXCEPTION '%', FORMAT('No data found in external table core.order_lines for date %s', p_date);
    END IF;

    UPDATE core.order_lines t
    SET valid_to = p_date - 1,
        is_current = FALSE
    FROM core.order_lines_ext s
    WHERE t.order_id = s.order_id
      AND t.product_id = s.product_id
      AND t.is_current = TRUE
      AND (
           t.order_line_id <> s.order_line_id
        OR t.quantity <> s.quantity
        OR t.price <> s.price
      );

    INSERT INTO core.order_lines (
        order_id, order_line_id, product_id, quantity, price,valid_from, valid_to, is_current
    )
    SELECT
        s.order_id, s.order_line_id, s.product_id, s.quantity, s.price, p_date, '9999-12-31', TRUE
    FROM core.order_lines_ext s
    LEFT JOIN core.order_lines t
        ON t.order_id = s.order_id
       AND t.product_id = s.product_id
       AND t.is_current = TRUE
    WHERE t.order_id IS NULL;

    DROP EXTERNAL TABLE IF EXISTS core.order_lines_ext;

    RETURN;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------