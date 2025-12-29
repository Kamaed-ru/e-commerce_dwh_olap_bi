from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState
from airflow.models import Variable
from airflow.hooks.base import BaseHook
import pendulum

OWNER = "omash"
DAG_ID = "stg_layer"

conn = BaseHook.get_connection("minio_s3")
env = {
    "AWS_ACCESS_KEY_ID": conn.login,
    "AWS_SECRET_ACCESS_KEY": conn.password,
    "S3_ENDPOINT_URL": conn.extra_dejson.get("endpoint_url"),
    "AWS_DEFAULT_REGION": conn.extra_dejson.get("region_name", "ru-central1"),
    "S3_BUCKET": Variable.get("S3_BUCKET"),
}

conn = BaseHook.get_connection("minio_s3")

args = {
    "owner": OWNER,
    "start_date": pendulum.parse(Variable.get("DAGS_START_DATE")).in_timezone("Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

def spark_task(task_id, job, cpu, mem):
    return DockerOperator(
        task_id=task_id,
        image="spark-ephemeral:3.5.7",
        command=[
            "/opt/spark/bin/spark-submit",
            f"/opt/spark_jobs/{job}",
            "{{ ds }}"
        ],
        environment=env,
        auto_remove="success",
        network_mode="bridge",
        mount_tmp_dir=False,
        do_xcom_push=False,
        cpus=cpu,
        mem_limit=mem,
    )

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 10 * * *",
    max_active_runs= 1,
    max_active_tasks=6,
    concurrency=6,
    tags=["stg", "spark", "ephemeral"],
) as dag:

    start = EmptyOperator(task_id="start")
    
    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="generate_raw_data",
        allowed_states=[DagRunState.SUCCESS],
        mode="reschedule",
        timeout=1800,
        poke_interval=10,
    )
  
    stg_cities = spark_task("stg_cities", "stg_cities_job.py", 0.5, "1g")
    stg_categories = spark_task("stg_categories", "stg_categories_job.py", 0.5, "1g")
    stg_customers = spark_task("stg_customers", "stg_customers_job.py", 1.0, "2g")
    stg_products = spark_task("stg_products", "stg_products_job.py", 1.0, "2g")
    stg_orders = spark_task("stg_orders", "stg_orders_job.py", 2.0, "4g")
    stg_order_lines = spark_task("stg_order_lines", "stg_order_lines_job.py", 2.0, "4g")

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> [
        stg_cities,
        stg_categories,
        stg_customers,
        stg_orders,
        stg_products,
        stg_order_lines,
    ] >> end