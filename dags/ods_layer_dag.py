from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.state import DagRunState
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pendulum

OWNER = "omash"
DAG_ID = "ods_layer_pxf_load"
START_DATE = pendulum.parse(Variable.get("DAGS_START_DATE")).in_timezone("Europe/Moscow")


args = {
    "owner": OWNER,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    start_date=START_DATE,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    concurrency=6,
    catchup=True,
    tags=["ods", "pxf", "greenplum"],
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="stg_layer",
        allowed_states=[DagRunState.SUCCESS],
        mode="reschedule",
        timeout=1800,
        poke_interval=10,
    )

    # === Cities ===
    cities_sql = """
    DROP EXTERNAL TABLE IF EXISTS ods.cities_ext;

    CREATE EXTERNAL TABLE ods.cities_ext (
        city_id INT,
        city_name TEXT,
        country TEXT,
        created_at DATE,
        deleted BOOLEAN
    )
    LOCATION ('pxf://ecommerce-data/stg/cities/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';
    
    UPDATE ods.cities t
    SET city_name = s.city_name,
        country = s.country,
        created_at = s.created_at,
        deleted = s.deleted
    FROM ods.cities_ext s
    WHERE t.city_id = s.city_id
    AND (
        t.city_name <> s.city_name
        OR t.country <> s.country
        OR t.created_at <> s.created_at
        OR t.deleted <> s.deleted
    );

    INSERT INTO ods.cities (city_id, city_name, country, created_at, deleted)
    SELECT s.city_id, s.city_name, s.country, s.created_at, s.deleted
    FROM ods.cities_ext s
    LEFT JOIN ods.cities t ON t.city_id = s.city_id
    WHERE t.city_id IS NULL;

    DROP EXTERNAL TABLE IF EXISTS ods.cities_ext;
    """

    ods_cities = SQLExecuteQueryOperator(
        task_id="ods_cities_load",
        conn_id="greenplum_db",
        autocommit=True,
        sql=cities_sql,
    )

    # === Categories ===
    categories_sql = """
    DROP EXTERNAL TABLE IF EXISTS ods.categories_ext;

    CREATE EXTERNAL TABLE ods.categories_ext (
        category_id INT,
        category_name TEXT,
        created_at DATE,
        deleted BOOLEAN
    )
    LOCATION ('pxf://ecommerce-data/stg/categories/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';

    UPDATE ods.categories t
    SET category_name = s.category_name,
        created_at = s.created_at,
        deleted = s.deleted
    FROM ods.categories_ext s
    WHERE t.category_id = s.category_id
    AND (
        t.category_name <> s.category_name
        OR t.created_at <> s.created_at
        OR t.deleted <> s.deleted
    );

    INSERT INTO ods.categories (category_id, category_name, created_at, deleted)
    SELECT s.category_id, s.category_name, s.created_at, s.deleted
    FROM ods.categories_ext s
    LEFT JOIN ods.categories t ON t.category_id = s.category_id
    WHERE t.category_id IS NULL;


    DROP EXTERNAL TABLE IF EXISTS ods.categories_ext;
    """

    ods_categories = SQLExecuteQueryOperator(
        task_id="ods_categories_load",
        conn_id="greenplum_db",
        autocommit=True,
        sql=categories_sql,
    )
    
    customers_sql = """
    DROP EXTERNAL TABLE IF EXISTS ods.customers_ext;

    CREATE EXTERNAL TABLE ods.customers_ext (
        customer_id INT,
        first_name  TEXT,
        last_name   TEXT,
        email       TEXT,
        city_id     INT,
        age         INT,
        gender      TEXT,
        registration_date DATE,
        updated_at  DATE,
        deleted     BOOLEAN,
        ingestion_date DATE
    )
    LOCATION ('pxf://ecommerce-data/stg/customers/ingestion_date={{ ds }}/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';

    UPDATE ods.customers t
    SET first_name = s.first_name,
        last_name = s.last_name,
        email = s.email,
        city_id = s.city_id,
        age = s.age,
        gender = s.gender,
        registration_date = s.registration_date,
        updated_at = s.updated_at,
        deleted = s.deleted,
        ingestion_date = s.ingestion_date
    FROM ods.customers_ext s
    WHERE t.customer_id = s.customer_id
    AND (
        t.first_name <> s.first_name
        OR t.last_name <> s.last_name
        OR t.email <> s.email
        OR t.city_id <> s.city_id
        OR t.age <> s.age
        OR t.gender <> s.gender
        OR t.registration_date <> s.registration_date
        OR t.updated_at <> s.updated_at
        OR t.deleted <> s.deleted
        OR t.ingestion_date <> s.ingestion_date
    );

    INSERT INTO ods.customers (customer_id, first_name, last_name, email, city_id, age, gender,
                            registration_date, updated_at, deleted, ingestion_date)
    SELECT s.customer_id, s.first_name, s.last_name, s.email, s.city_id, s.age, s.gender,
        s.registration_date, s.updated_at, s.deleted, s.ingestion_date
    FROM ods.customers_ext s
    LEFT JOIN ods.customers t ON t.customer_id = s.customer_id
    WHERE t.customer_id IS NULL;


    DROP EXTERNAL TABLE ods.customers_ext;
    """

    ods_customers = SQLExecuteQueryOperator(
        task_id="ods_customers_load",
        conn_id="greenplum_db",
        autocommit=True,
        sql=customers_sql,
    )

    products_sql = """
    DROP EXTERNAL TABLE IF EXISTS ods.products_ext;

    CREATE EXTERNAL TABLE ods.products_ext (
        product_id INT,
        product_name TEXT,
        category_id INT,
        price DECIMAL(10,2),
        weight_kg FLOAT,
        updated_at DATE,
        deleted BOOLEAN,
        ingestion_date DATE
    )
    LOCATION ('pxf://ecommerce-data/stg/products/ingestion_date={{ ds }}/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';

    UPDATE ods.products t
    SET product_name = s.product_name,
        category_id = s.category_id,
        price = s.price,
        weight_kg = s.weight_kg,
        updated_at = s.updated_at,
        deleted = s.deleted,
        ingestion_date = s.ingestion_date
    FROM ods.products_ext s
    WHERE t.product_id = s.product_id
    AND (
        t.product_name <> s.product_name
        OR t.category_id <> s.category_id
        OR t.price <> s.price
        OR t.weight_kg <> s.weight_kg
        OR t.updated_at <> s.updated_at
        OR t.deleted <> s.deleted
        OR t.ingestion_date <> s.ingestion_date
    );

    INSERT INTO ods.products (product_id, product_name, category_id, price, weight_kg, updated_at, deleted, ingestion_date)
    SELECT s.product_id, s.product_name, s.category_id, s.price, s.weight_kg, s.updated_at, s.deleted, s.ingestion_date
    FROM ods.products_ext s
    LEFT JOIN ods.products t ON t.product_id = s.product_id
    WHERE t.product_id IS NULL;


    DROP EXTERNAL TABLE ods.products_ext;
    """

    ods_products = SQLExecuteQueryOperator(
        task_id="ods_products_load",
        conn_id="greenplum_db",
        autocommit=True,
        sql=products_sql,
    )

    orders_sql = """
    DROP EXTERNAL TABLE IF EXISTS ods.orders_ext;

    CREATE EXTERNAL TABLE ods.orders_ext (
        order_id BIGINT,
        customer_id INT,
        order_date DATE,
        order_status TEXT,
        ingestion_date DATE
    )
    LOCATION ('pxf://ecommerce-data/stg/orders/ingestion_date={{ ds }}/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';


    INSERT INTO ods.orders
    (order_id, customer_id, order_date, order_status, ingestion_date)
    SELECT order_id, customer_id, order_date, order_status, ingestion_date FROM ods.orders_ext;

    DROP EXTERNAL TABLE ods.orders_ext;
    """


    ods_orders = SQLExecuteQueryOperator(
        task_id="ods_orders_load",
        conn_id="greenplum_db",
        autocommit=True,
        sql=orders_sql,
    )
    
    order_lines_sql = """
    DROP EXTERNAL TABLE IF EXISTS ods.order_lines_ext;

    CREATE EXTERNAL TABLE ods.order_lines_ext (
        order_id BIGINT,
        order_line_id INT,
        product_id INT,
        quantity INT,
        price DECIMAL(10,2),
        ingestion_date DATE
    )
    LOCATION ('pxf://ecommerce-data/stg/order_lines/ingestion_date={{ ds }}/*.parquet?PROFILE=s3:parquet&SERVER=minio')
    FORMAT 'CUSTOM' (formatter='pxfwritable_import')
    ENCODING 'UTF8';

    INSERT INTO ods.order_lines
    (order_id, order_line_id, product_id, quantity, price, ingestion_date)
    SELECT order_id, order_line_id, product_id, quantity, price, ingestion_date FROM ods.order_lines_ext;

    DROP EXTERNAL TABLE ods.order_lines_ext;
    """

    ods_order_lines = SQLExecuteQueryOperator(
        task_id="ods_order_lines_load",
        conn_id="greenplum_db",
        autocommit=True,
        sql=order_lines_sql,
    )
    
    end = EmptyOperator(task_id="end")



start >> sensor_on_raw_layer >> [ods_cities, ods_categories, ods_customers, ods_products, ods_orders, ods_order_lines] >> end
