from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import pendulum

OWNER = "omash"
DAG_ID = "gp_to_s3_export"

conn_minio = BaseHook.get_connection("minio_s3")
conn_gp = BaseHook.get_connection("greenplum_db")

env = {
    # S3 / MinIO
    "AWS_ACCESS_KEY_ID": conn_minio.login,
    "AWS_SECRET_ACCESS_KEY": conn_minio.password,
    "S3_ENDPOINT_URL": conn_minio.extra_dejson.get("endpoint_url"),
    "AWS_DEFAULT_REGION": conn_minio.extra_dejson.get("region_name", "ru-central1"),
    "S3_BUCKET": Variable.get("S3_BUCKET"),

    # Greenplum JDBC
    "GP_JDBC_URL": f"jdbc:postgresql://{conn_gp.host}:{conn_gp.port}/{conn_gp.schema}",
    "GP_USER": conn_gp.login,
    "GP_PASSWORD": conn_gp.password,
}
args = {
    "owner": OWNER,
    "start_date": pendulum.parse(Variable.get("DAGS_START_DATE")).in_timezone("Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

def spark_job(task_id, job_name, cpu, mem):
    return DockerOperator(
        task_id=task_id,
        image="spark-ephemeral:3.5.7",
        command=[
            "/opt/spark/bin/spark-submit",
            f"/opt/spark_jobs/{job_name}",
            "{{ ds }}"
        ],
        environment=env,
        auto_remove="success",
        network_mode="e-commerce_dwh_olap_bi_default",
        mount_tmp_dir=False,
        do_xcom_push=False,
        cpus=cpu,
        mem_limit=mem,
    )

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    max_active_tasks=6,
    concurrency=6,
    tags=["mart", "spark", "greenplum", "export"],
) as dag:

    start = EmptyOperator(task_id="start")

    sensor_on_stg = ExternalTaskSensor(
        task_id="sensor_on_stg",
        external_dag_id="mart_layer",
        allowed_states=[DagRunState.SUCCESS],
        mode="reschedule",
        timeout=1800,
        poke_interval=10,
    )

    # dimensions SCD2
    t_customers = spark_job("fill_customers", "mart_export_customers_job.py", 1.0, "2g")
    t_products = spark_job("fill_products", "mart_export_products_job.py", 1.0, "2g")
    t_daily_sales = spark_job("fill_daily_sales", "mart_export_daily_sales_job.py", 2.0, "4g")
    t_customer_ltv = spark_job("fill_customer_ltv", "mart_export_customer_ltv_job.py", 2.0, "4g")
    t_product_popularity = spark_job("fill_product_popularity", "mart_export_product_popularity_job.py", 2.0, "4g")
    t_retention_metrics = spark_job("fill_retention_metrics", "mart_export_retention_metrics_job.py", 1.0, "1g")

    end = EmptyOperator(task_id="end")

    start >> sensor_on_stg >> [ t_customers, t_products, t_daily_sales, t_customer_ltv, t_product_popularity, t_retention_metrics] >> end