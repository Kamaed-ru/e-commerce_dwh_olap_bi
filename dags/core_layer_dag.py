from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
import pendulum

OWNER = "omash"
DAG_ID = "core_layer_dag"

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
    max_active_runs= 1,
    max_active_tasks=6,
    concurrency=6,
    tags=["ods", "pxf", "greenplum"],
) as dag:

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="stg_layer",
        allowed_states=["success"],
        mode="reschedule",
        timeout=1800,
        poke_interval=10,
    )
    
    call_cities = SQLExecuteQueryOperator(
        task_id="fill_cities",
        conn_id="greenplum_db",
        sql="SELECT core.fill_cities();",
        autocommit=True,
    )
    
    call_categories = SQLExecuteQueryOperator(
        task_id="fill_categories",
        conn_id="greenplum_db",
        sql="SELECT core.fill_categories();",
        autocommit=True,
    )

    call_customers = SQLExecuteQueryOperator(
        task_id="fill_customers",
        conn_id="greenplum_db",
        sql="SELECT core.fill_customers('{{ ds }}'::DATE);",
        autocommit=True,
    )

    call_products = SQLExecuteQueryOperator(
        task_id="fill_products",
        conn_id="greenplum_db",
        sql="SELECT core.fill_products('{{ ds }}'::DATE);",
        autocommit=True,
    )

    call_orders = SQLExecuteQueryOperator(
        task_id="fill_orders",
        conn_id="greenplum_db",
        sql="SELECT core.fill_orders('{{ ds }}'::DATE);",
        autocommit=True,
    )

    call_order_lines = SQLExecuteQueryOperator(
        task_id="fill_order_lines",
        conn_id="greenplum_db",
        sql="SELECT core.fill_order_lines('{{ ds }}'::DATE);",
        autocommit=True,
    )

    end = EmptyOperator(task_id="end")

    # порядок
    start >> sensor_on_raw_layer >> [call_cities, call_categories, call_customers, call_products, call_orders,call_order_lines] >> end
