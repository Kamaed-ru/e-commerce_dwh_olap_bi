from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pendulum

OWNER = "omash"
DAG_ID = "mart_layer_dag"

args = {
    "owner": OWNER,
    "start_date": pendulum.parse(Variable.get("DAGS_START_DATE")).in_timezone("Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1)
}

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    max_active_tasks=4,
    concurrency=4,
    tags=["core", "mart", "greenplum"],
) as dag:

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="core_layer_dag",
        allowed_states=["success"],
        mode="reschedule",
        timeout=1800,
        poke_interval=10,
    )

    fill_daily_sales = SQLExecuteQueryOperator(
        task_id="fill_daily_sales",
        conn_id="greenplum_db",
        sql="SELECT mart.fill_daily_sales('{{ ds }}'::DATE);",
        autocommit=True,
    )
    
    fill_customer_ltv = SQLExecuteQueryOperator(
        task_id="fill_customer_ltv",
        conn_id="greenplum_db",
        sql="SELECT mart.fill_customer_ltv('{{ ds }}'::DATE);",
        autocommit=True,
    )
    
    fill_product_popularity = SQLExecuteQueryOperator(
        task_id="fill_product_popularity",
        conn_id="greenplum_db",
        sql="SELECT mart.fill_product_popularity('{{ ds }}'::DATE);",
        autocommit=True,
    )
    
    fill_retention_metrics = SQLExecuteQueryOperator(
        task_id="fill_retention_metrics",
        conn_id="greenplum_db",
        sql="SELECT mart.fill_retention_metrics('{{ ds }}'::DATE);",
        autocommit=True,
    )

    end = EmptyOperator(task_id="end")

    # порядок: ждём STG → заполняем Mart → конец
    start >> sensor_on_raw_layer >> fill_daily_sales >> fill_customer_ltv >> fill_product_popularity >> fill_retention_metrics >> end
