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
-------------------------------------------------------------------------------------------
-- ============ DIMENSIONS (customers, products) ============
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
-------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.products (
    product_sk BIGSERIAL PRIMARY KEY,
    product_id INT NOT NULL,
    product_name VARCHAR(80),
    category_id INT,
    price DECIMAL(10,2),
    weight DECIMAL(10,2),
    updated_at DATE,
    deleted BOOLEAN,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
) DISTRIBUTED REPLICATED;
-------------------------------------------------------------------------------------------
-- ============ FACTS (orders, order_lines) ============
CREATE TABLE IF NOT EXISTS core.orders (
    order_sk      BIGSERIAL,
    order_id      BIGINT NOT NULL,
    customer_id   INT    NOT NULL,
    order_date    DATE   NOT NULL,
    order_status  VARCHAR(12)
)
WITH (appendoptimized = true, orientation = column)
DISTRIBUTED BY (order_id)
PARTITION BY RANGE (order_date) (
    START (DATE '2025-01-01')
    END   (DATE '2026-05-01')
    EVERY (INTERVAL '1 week')
);
-------------------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS core.order_lines (
    order_line_sk BIGSERIAL,
    order_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    order_line_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT,
    price DECIMAL(10,2)
)
WITH (appendoptimized = true, orientation = column)
DISTRIBUTED BY (order_id)
PARTITION BY RANGE (order_date) (
    START (DATE '2025-01-01')
    END   (DATE '2026-05-01')
    EVERY (INTERVAL '1 week')
);
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- CORE FILL FUNCTIONS FOR CORE TABLES
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
        RAISE NOTICE '⚠ No data in STG for cities on date %, skipping load', p_date;
        DROP EXTERNAL TABLE core.cities_ext;
        RETURN;
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
        RAISE NOTICE '⚠ No data in STG for categories on date %, skipping load', p_date;
        DROP EXTERNAL TABLE core.categories_ext;
        RETURN;
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
            weight DECIMAL(10,2),
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
        OR t.weight <> s.weight
        OR t.deleted <> s.deleted
      );

    INSERT INTO core.products (
        product_id, product_name, category_id, price, weight,
        updated_at, deleted,
        valid_from, valid_to, is_current
    )
    SELECT
        s.product_id, s.product_name, s.category_id, s.price, s.weight,
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

    INSERT INTO core.orders (
        order_id, customer_id, order_date, order_status
    )
    SELECT
        s.order_id, s.customer_id, s.order_date, s.order_status
    FROM core.orders_ext s;

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


    INSERT INTO core.order_lines (
        order_id, order_date, order_line_id, product_id, quantity, price
    )
    SELECT
        s.order_id, p_date, s.order_line_id, s.product_id, s.quantity, s.price
    FROM core.order_lines_ext s;

    DROP EXTERNAL TABLE IF EXISTS core.order_lines_ext;

    RETURN;
END;
$$ LANGUAGE plpgsql;

-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- MART Schema and Tables Initialization Script
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS mart;

CREATE TABLE mart.daily_sales (
    sales_date DATE NOT NULL,
    city_id INT,
    product_id INT,
    orders_count BIGINT,
    total_quantity BIGINT,
    total_revenue DECIMAL(18,2),
    avg_order_value DECIMAL(18,2),
    load_date DATE DEFAULT CURRENT_DATE
)
WITH (appendoptimized = true, orientation = column)
DISTRIBUTED BY (sales_date, city_id, product_id)
PARTITION BY RANGE (sales_date) (
    START (DATE '2025-01-01')
    END   (DATE '2026-05-01')
    EVERY (INTERVAL '1 week')
);
-------------------------------------------------------------------------------------------
CREATE TABLE mart.customer_ltv (
    customer_id INT NOT NULL,
    first_order DATE,
    last_order DATE,
    lifetime_days INT,
    orders_count BIGINT,
    total_spent DECIMAL(18,2),
    avg_check DECIMAL(18,2),
    churn_risk BOOLEAN DEFAULT FALSE,
    load_date DATE DEFAULT CURRENT_DATE
) DISTRIBUTED BY (customer_id)
PARTITION BY RANGE (load_date) (
    START (DATE '2025-01-01')
    END   (DATE '2026-05-01')
    EVERY (INTERVAL '1 week')
);
-------------------------------------------------------------------------------------------
CREATE TABLE mart.product_popularity (
    product_id INT NOT NULL,
    category_id INT,
    orders_count BIGINT,
    total_quantity BIGINT,
    popularity_rank INT,
    revenue_share DECIMAL(5,2),
    load_date DATE DEFAULT CURRENT_DATE
) DISTRIBUTED BY (product_id)
PARTITION BY RANGE (load_date) (
    START (DATE '2025-01-01')
    END   (DATE '2026-05-01')
    EVERY (INTERVAL '1 week')
);
-------------------------------------------------------------------------------------------
CREATE TABLE mart.retention_metrics (
    cohort_month DATE NOT NULL,
    active_month DATE NOT NULL,
    customers_count BIGINT,
    retained_count BIGINT,
    retention_rate DECIMAL(5,2),
    load_date DATE DEFAULT CURRENT_DATE
) DISTRIBUTED BY (cohort_month)
PARTITION BY RANGE (load_date) (
    START (DATE '2025-01-01')
    END   (DATE '2026-05-01')
    EVERY (INTERVAL '1 week')
);
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------
-- FILL FUNCTIONS FOR MART TABLES
-------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------

-- 1. Daily Sales
CREATE OR REPLACE FUNCTION mart.fill_daily_sales(p_date DATE)
RETURNS VOID AS $$
BEGIN
    DELETE FROM mart.daily_sales WHERE sales_date = p_date;

    INSERT INTO mart.daily_sales (
        sales_date, city_id, product_id,
        orders_count, total_quantity, total_revenue, avg_order_value, load_date
    )
    SELECT
        p_date AS sales_date,
        c.city_id,
        p.product_id,
        COUNT(o.order_id) AS orders_count,
        COALESCE(SUM(l.quantity), 0) AS total_quantity,
        COALESCE(SUM(l.price * l.quantity), 0) AS total_revenue,
        COALESCE(SUM(l.price * l.quantity) / NULLIF(COUNT(o.order_id), 0), 0) AS avg_order_value,
        p_date AS load_date
    FROM core.orders o
    JOIN core.order_lines l ON l.order_id = o.order_id and l.order_date = o.order_date
    JOIN core.customers cu ON cu.customer_id = o.customer_id AND cu.is_current = TRUE
    JOIN core.cities c ON c.city_id = cu.city_id
    JOIN core.products p ON p.product_id = l.product_id AND p.is_current = TRUE
    WHERE o.order_date = p_date
    GROUP BY c.city_id, p.product_id;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------
-- 2. Customer LTV
CREATE OR REPLACE FUNCTION mart.fill_customer_ltv(p_date DATE)
RETURNS VOID AS $$
BEGIN
    DELETE FROM mart.customer_ltv WHERE load_date = p_date;

    INSERT INTO mart.customer_ltv (
        customer_id, first_order, last_order, lifetime_days,
        orders_count, total_spent, avg_check, churn_risk, load_date
    )
    SELECT
        cu.customer_id,
        MIN(o.order_date) AS first_order,
        MAX(o.order_date) AS last_order,
        (MAX(o.order_date) - MIN(o.order_date)) + 1 AS lifetime_days,
        COUNT(o.order_id) AS orders_count,
        COALESCE(SUM(l.price * l.quantity), 0) AS total_spent,
        COALESCE(SUM(l.price * l.quantity) / NULLIF(COUNT(o.order_id), 0), 0) AS avg_check,
        CASE 
            WHEN MAX(o.order_date) < p_date - INTERVAL '30 days' THEN TRUE 
            ELSE FALSE 
        END AS churn_risk,
        p_date AS load_date
    FROM core.orders o
    JOIN core.order_lines l 
        ON l.order_id = o.order_id 
       AND l.order_date = o.order_date
    JOIN core.customers cu 
        ON cu.customer_id = o.customer_id
       AND cu.is_current = TRUE
    WHERE o.order_date <= p_date
    GROUP BY cu.customer_id;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------
-- 3. Product Popularity
CREATE OR REPLACE FUNCTION mart.fill_product_popularity(p_date DATE)
RETURNS VOID AS $$
BEGIN
    CREATE TEMP TABLE tmp_pop AS
    SELECT
        p.product_id,
        p.category_id,
        COUNT(o.order_id) AS orders_count,
        SUM(l.quantity) AS total_quantity,
        SUM(l.quantity * l.price) AS revenue
    FROM core.orders o
    JOIN core.order_lines l ON l.order_id = o.order_id and l.order_date = o.order_date
    JOIN core.products p ON p.product_id = l.product_id AND p.is_current = TRUE
    WHERE o.order_date = p_date
    GROUP BY p.product_id, p.category_id;

    IF NOT EXISTS (SELECT 1 FROM tmp_pop LIMIT 1) THEN
        DROP TABLE IF EXISTS tmp_pop;
        RETURN;
    END IF;

    WITH total AS (
        SELECT SUM(revenue) AS total_revenue FROM tmp_pop
    )
    INSERT INTO mart.product_popularity (
        product_id,
        category_id,
        orders_count,
        total_quantity,
        popularity_rank,
        revenue_share,
        load_date
    )
    SELECT
        t.product_id,
        t.category_id,
        t.orders_count,
        t.total_quantity,
        RANK() OVER (ORDER BY t.orders_count DESC) AS popularity_rank,
        ROUND(t.revenue * 100.0 / total.total_revenue, 2) AS revenue_share,
        p_date AS load_date
    FROM tmp_pop t, total
    WHERE total.total_revenue > 0;

    DROP TABLE IF EXISTS tmp_pop;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------
-- 4. Retention Metrics
CREATE OR REPLACE FUNCTION mart.fill_retention_metrics(p_date DATE)
RETURNS VOID AS $$
BEGIN
    DELETE FROM mart.retention_metrics WHERE load_date = p_date;

    INSERT INTO mart.retention_metrics (
        cohort_month,
        active_month,
        customers_count,
        retained_count,
        retention_rate,
        load_date
    )
    WITH cohorts AS (
        SELECT
            DATE_TRUNC('month', registration_date)::DATE AS cohort_month,
            customer_id
        FROM core.customers
        WHERE deleted = FALSE
        GROUP BY DATE_TRUNC('month', registration_date)::DATE, customer_id
    ),
    activity AS (
        SELECT
            DATE_TRUNC('month', order_date)::DATE AS active_month,
            customer_id
        FROM core.orders
        WHERE order_date <= p_date
        GROUP BY DATE_TRUNC('month', order_date)::DATE, customer_id
    )
    SELECT
        co.cohort_month,
        ac.active_month,
        COUNT(co.customer_id) AS customers_count,
        COUNT(ac.customer_id) AS retained_count,
        ROUND(
            100.0 * COUNT(ac.customer_id) / NULLIF(COUNT(co.customer_id), 0),
            2
        ) AS retention_rate,
        p_date AS load_date
    FROM cohorts co
    INNER JOIN activity ac
        ON ac.customer_id = co.customer_id
    GROUP BY co.cohort_month, ac.active_month;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------
