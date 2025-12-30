from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
import pendulum

OWNER = "omash"
DAG_ID = "mart_s3_to_ch"

args = {
    "owner": OWNER,
    "start_date": pendulum.parse(Variable.get("DAGS_START_DATE")).in_timezone("Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1)
}

conn_minio = BaseHook.get_connection("minio_s3")

AWS_ACCESS_KEY_ID=conn_minio.login
AWS_SECRET_ACCESS_KEY=conn_minio.password
S3_ENDPOINT_URL=conn_minio.extra_dejson.get("endpoint_url")
S3_BUCKET=Variable.get("S3_BUCKET")

s3_customers_path = f"{S3_ENDPOINT_URL}/{S3_BUCKET}/mart/dim/customers/*.parquet"
s3_products_path  = f"{S3_ENDPOINT_URL}/{S3_BUCKET}/mart/dim/products/*.parquet"

s3_daily_sales_path = f"{S3_ENDPOINT_URL}/{S3_BUCKET}/mart/daily_sales/load_date={{{{ ds }}}}/*.parquet"
s3_customer_ltv_path = f"{S3_ENDPOINT_URL}/{S3_BUCKET}/mart/customer_ltv/load_date={{{{ ds }}}}/*.parquet"
s3_product_popularity_path = f"{S3_ENDPOINT_URL}/{S3_BUCKET}/mart/product_popularity/load_date={{{{ ds }}}}/*.parquet"
s3_retention_metrics_path = f"{S3_ENDPOINT_URL}/{S3_BUCKET}/mart/retention_metrics/load_date={{{{ ds }}}}/*.parquet"

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    max_active_tasks=6,
    concurrency=6,
    tags=["core", "mart", "greenplum"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    truncate_customers = ClickHouseOperator(
        task_id="truncate_customers",
        clickhouse_conn_id="clickhouse_default",
        sql="""
            TRUNCATE TABLE customers_mt;
       """
    )

    load_customers = ClickHouseOperator(
        task_id="load_customers",
        clickhouse_conn_id="clickhouse_default",
        sql=f"""
        INSERT INTO customers_mt
        SELECT
            customer_id,
            first_name,
            last_name,
            email,
            city_id,
            age,
            gender,
            registration_date,
            updated_at,
            deleted,
            valid_from,
            if(valid_to > 65535, toDate('2149-06-06'), addDays(toDate('1970-01-01'), valid_to)) AS valid_to,
            is_current
        FROM s3(
            '{s3_customers_path}',
            '{AWS_ACCESS_KEY_ID}',
            '{AWS_SECRET_ACCESS_KEY}',
            'Parquet',
            'customer_sk UInt64, customer_id UInt32, first_name Nullable(String), last_name Nullable(String),
             email Nullable(String), city_id Nullable(UInt32), age Nullable(UInt32), gender Nullable(FixedString(1)),
             registration_date Nullable(Date), updated_at Nullable(Date), deleted Nullable(Bool),
             valid_from Nullable(Date), valid_to Int32, is_current Nullable(Bool)'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1;
    """
    )
    
    truncate_products = ClickHouseOperator(
        task_id="truncate_products",
        clickhouse_conn_id="clickhouse_default",
        sql="""
            TRUNCATE TABLE products_mt;
       """
    )

    load_products = ClickHouseOperator(
        task_id="load_products",
        clickhouse_conn_id="clickhouse_default",
        sql=f"""
        INSERT INTO products_mt
        SELECT
            product_id,
            product_name,
            category_id,
            price,
            weight,
            updated_at,
            deleted,
            valid_from,
            if(valid_to > 65535, toDate('2149-06-06'), addDays(toDate('1970-01-01'), valid_to)) AS valid_to,
            is_current
        FROM s3(
            '{s3_products_path}',
            '{AWS_ACCESS_KEY_ID}',
            '{AWS_SECRET_ACCESS_KEY}',
            'Parquet',
            'product_sk UInt64, product_id UInt32, product_name Nullable(String), category_id Nullable(UInt32),
             price Nullable(Decimal(10,2)), weight Nullable(Decimal(10,2)), updated_at Nullable(Date),
             deleted Nullable(Bool), valid_from Nullable(Date), valid_to Int32, is_current Nullable(Bool)'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1;
    """
    )
    
    load_daily_sales = ClickHouseOperator(
        task_id="load_daily_sales",
        clickhouse_conn_id="clickhouse_default",
        sql=f"""
        INSERT INTO daiy_sales
        SELECT *
        FROM s3(
            '{s3_daily_sales_path}',
            '{AWS_ACCESS_KEY_ID}',
            '{AWS_SECRET_ACCESS_KEY}',
            'Parquet',
            'sales_date Date32, city_id Int32, product_id Int32, orders_count Nullable(Int64),
             total_quantity Nullable(Int64), total_revenue Nullable(Decimal(18,2)),
             avg_order_value Nullable(Decimal(18,2)), load_date Nullable(Date32)'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1;
        """
    )

    load_customer_ltv = ClickHouseOperator(
        task_id="load_customer_ltv",
        clickhouse_conn_id="clickhouse_default",
        sql=f"""
        INSERT INTO customer_ltv
        SELECT *
        FROM s3(
            '{s3_customer_ltv_path}',
            '{AWS_ACCESS_KEY_ID}',
            '{AWS_SECRET_ACCESS_KEY}',
            'Parquet',
            'customer_id Int32, first_order Nullable(Date32), last_order Nullable(Date32),
             lifetime_days Nullable(Int32), orders_count Nullable(Int64),
             total_spent Nullable(Decimal(18,2)), avg_check Nullable(Decimal(18,2)),
             churn_risk Nullable(Bool), load_date Date32'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1;
        """
    )

    load_product_popularity = ClickHouseOperator(
        task_id="load_product_popularity",
        clickhouse_conn_id="clickhouse_default",
        sql=f"""
        INSERT INTO product_popularity
        SELECT *
        FROM s3(
            '{s3_product_popularity_path}',
            '{AWS_ACCESS_KEY_ID}',
            '{AWS_SECRET_ACCESS_KEY}',
            'Parquet',
            'product_id Int32, category_id Int32, orders_count Nullable(Int64),
             total_quantity Nullable(Int64), popularity_rank Nullable(Int32),
             revenue_share Nullable(Decimal(5,2)), load_date Date32'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1;
        """
    )

    load_retention_metrics = ClickHouseOperator(
        task_id="load_retention_metrics",
        clickhouse_conn_id="clickhouse_default",
        sql=f"""
        INSERT INTO retention_metrics
        SELECT *
        FROM s3(
            '{s3_retention_metrics_path}',
            '{AWS_ACCESS_KEY_ID}',
            '{AWS_SECRET_ACCESS_KEY}',
            'Parquet',
            'cohort_month Date32, active_month Nullable(Date32), customers_count Nullable(Int64),
             retained_count Nullable(Int64), retention_rate Nullable(Decimal(5,2)), load_date Date32'
        )
        SETTINGS input_format_parquet_allow_missing_columns = 1;
        """
    )
    
    
    
    end = EmptyOperator(task_id="end")

    start >> [truncate_customers, 
            truncate_products, 
            load_daily_sales,
            load_customer_ltv,
            load_product_popularity,
            load_retention_metrics]

    truncate_customers >> load_customers
    truncate_products  >> load_products

    [
        load_customers,
        load_products,
        load_daily_sales,
        load_customer_ltv,
        load_product_popularity,
        load_retention_metrics
    ] >> end