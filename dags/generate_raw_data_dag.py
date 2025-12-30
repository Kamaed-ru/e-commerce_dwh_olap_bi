from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
from faker import Faker
import pendulum
import json
import random
import io
import gzip

OWNER = "omash"
DAG_ID = "generate_raw_data"

S3_BUCKET = Variable.get("S3_BUCKET")
S3_CONN_ID = "minio_s3"

fake = Faker()
Faker.seed(42)
random.seed(42)

START_CUSTOMERS = 5_000
START_PRODUCTS = 10_000
CITIES_COUNT = 100
CATEGORIES_COUNT = 100

ORDERS_PER_DAY = 100_000
MAX_LINES_PER_ORDER = 5

args = {
    "owner": OWNER,
    "start_date": pendulum.parse(Variable.get("DAGS_START_DATE")).in_timezone("Europe/Moscow"),
    "catchup": True,
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
}

# =========================================================
# ====================== HELPERS ==========================
# =========================================================

def s3_read_json_gz(key: str):
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    if not s3.check_for_key(key, S3_BUCKET):
        return None

    obj = s3.get_key(key, S3_BUCKET)
    raw_bytes = obj.get()["Body"].read()

    with gzip.GzipFile(fileobj=io.BytesIO(raw_bytes), mode="rb") as gz:
        data = gz.read()

    return json.loads(data.decode("utf-8"))


def s3_write_json_gz(key: str, payload):
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)

    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    buf.seek(0)

    s3.load_bytes(
        bytes_data=buf.read(),
        key=key,
        bucket_name=S3_BUCKET,
        replace=True,
    )

# =========================================================
# =================== REFERENCE DATA ======================
# =========================================================

def generate_cities():
    cities = [
        {
            "city_id": i,
            "city_name": fake.city(),
            "country": "USA",
            "created_at": str(pendulum.today().date()),
            "deleted": False,
        }
        for i in range(1, CITIES_COUNT + 1)
    ]

    s3_write_json_gz("raw/cities/cities.json.gz", cities)


def generate_categories():
    categories = [
        {
            "category_id": i,
            "category_name": fake.word().title(),
            "created_at": str(pendulum.today().date()),
            "deleted": False,
        }
        for i in range(1, CATEGORIES_COUNT + 1)
    ]

    s3_write_json_gz("raw/categories/categories.json.gz", categories)

# =========================================================
# =================== DIMENSIONS ==========================
# =========================================================

from datetime import datetime, timedelta

def generate_customers(**context):
    ds = context["ds"]  # YYYY-MM-DD (сегодняшняя дата запуска)
    ds_date = datetime.strptime(ds, "%Y-%m-%d")

    # регистрация — случайный день в диапазоне [ds - 365 дней, ds]
    start_reg = ds_date - timedelta(days=365)
    
    def random_registration():
        delta_days = random.randint(0, 365)
        return (start_reg + timedelta(days=delta_days)).strftime("%Y-%m-%d")

    prev_ds = context["data_interval_start"].subtract(days=1).to_date_string()
    prev_key = f"raw/customers/date={prev_ds}/customers.json.gz"
    curr_key = f"raw/customers/date={ds}/customers.json.gz"

    prev_customers = s3_read_json_gz(prev_key)

    if prev_customers is None:
        customers = [
            {
                "customer_id": i,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": f"user{i}@example.com",
                "city_id": random.randint(1, CITIES_COUNT),
                "age": random.randint(18, 70),
                "gender": random.choice(["M", "F"]),
                "registration_date": random_registration(),
                "updated_at": ds,
                "deleted": False,
            }
            for i in range(1, START_CUSTOMERS + 1)
        ]
    else:
        customers = prev_customers.copy()
        active = [c for c in customers if not c["deleted"]]

        if active:
            rc1, rc2 = random.sample(active, 2) if len(active) > 1 else (active[0], active[0])

            rc1["age"] += 1
            rc1["updated_at"] = ds

            rc2["deleted"] = True
            rc2["updated_at"] = ds

        new_id = max(c["customer_id"] for c in customers) + 1
        customers.append(
            {
                "customer_id": new_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": f"user{new_id}@example.com",
                "city_id": random.randint(1, CITIES_COUNT),
                "age": random.randint(18, 70),
                "gender": random.choice(["M", "F"]),
                "registration_date": random_registration(),
                "updated_at": ds,
                "deleted": False,
            }
        )

    s3_write_json_gz(curr_key, customers)


def generate_products(**context):
    ds = context["ds"]
    prev_ds = context["data_interval_start"].subtract(days=1).to_date_string()

    prev_key = f"raw/products/date={prev_ds}/products.json.gz"
    curr_key = f"raw/products/date={ds}/products.json.gz"

    prev_products = s3_read_json_gz(prev_key)

    if prev_products is None:
        products = [
            {
                "product_id": i,
                "product_name": fake.catch_phrase(),
                "category_id": random.randint(1, CATEGORIES_COUNT),
                "price": round(random.uniform(5, 500), 2),
                "weight": round(random.uniform(0, 20), 2),
                "is_active": True,
                "created_at": ds,
                "updated_at": ds,
                "deleted": False,
            }
            for i in range(1, START_PRODUCTS + 1)
        ]
    else:
        products = prev_products.copy()
        active = [p for p in products if not p["deleted"]]

        p = random.choice(active)
        p["price"] = round(p["price"] * random.uniform(0.95, 1.05), 2)
        p["updated_at"] = ds

        d = random.choice(active)
        d["deleted"] = True
        d["is_active"] = False
        d["updated_at"] = ds

        new_id = max(p["product_id"] for p in products) + 1
        products.append(
            {
                "product_id": new_id,
                "product_name": fake.catch_phrase(),
                "category_id": random.randint(1, CATEGORIES_COUNT),
                "price": round(random.uniform(5, 500), 2),
                "weight": round(random.uniform(0, 20), 2),
                "is_active": True,
                "created_at": ds,
                "updated_at": ds,
                "deleted": False,
            }
        )

    s3_write_json_gz(curr_key, products)

# =========================================================
# ====================== FACTS ============================
# =========================================================

def generate_orders(**context):
    ds = context["ds"]

    customers = s3_read_json_gz(f"raw/customers/date={ds}/customers.json.gz")
    products = s3_read_json_gz(f"raw/products/date={ds}/products.json.gz")

    active_customers = [c for c in customers if not c["deleted"]]
    active_products = [p for p in products if not p["deleted"]]
    
    # --- Учёт вчерашнего максимального order_id ---
    yesterday = pendulum.parse(ds).subtract(days=1).to_date_string()
    prev_orders = s3_read_json_gz(f"raw/orders/date={yesterday}/orders.json.gz") or []
    order_id = max((o["order_id"] for o in prev_orders), default=0)

    orders = []
    order_lines = []

    for i in range(1, ORDERS_PER_DAY + 1):
        customer = random.choice(active_customers)
        order_id += 1
        
        orders.append(
            {
                "order_id": order_id,
                "order_date": ds,
                "customer_id": customer["customer_id"],
                "order_status": random.choice(["NEW", "PAID", "SHIPPED"]),
                "created_at": ds,
            }
        )

        for p in range(1, random.randint(1, MAX_LINES_PER_ORDER)):
            products = random.choice(active_products)
            
            order_lines.append(
                {
                    "order_id": order_id,
                    "order_line_id": p,
                    "product_id": products["product_id"],
                    "quantity": random.randint(1, 5),
                    "price": products["price"],
                }
            )

    s3_write_json_gz(f"raw/orders/date={ds}/orders.json.gz", orders)
    s3_write_json_gz(f"raw/order_lines/date={ds}/order_lines.json.gz", order_lines)

# =========================================================
# ======================== DAG ============================
# =========================================================

with DAG(
    dag_id=DAG_ID,
    default_args=args,
    schedule_interval="0 10 * * *",
    tags=["generator", "raw"],
    max_active_runs= 1,
    max_active_tasks=6,
    concurrency=6
) as dag:

    start = EmptyOperator(task_id="start")

    gen_cities = PythonOperator(task_id="generate_cities", python_callable=generate_cities)
    gen_categories = PythonOperator(task_id="generate_categories", python_callable=generate_categories)

    gen_customers = PythonOperator(task_id="generate_customers", python_callable=generate_customers)
    gen_products = PythonOperator(task_id="generate_products", python_callable=generate_products)

    gen_orders = PythonOperator(task_id="generate_orders_and_lines", python_callable=generate_orders)

    end = EmptyOperator(task_id="end")

    start >> [gen_cities, gen_categories, gen_customers, gen_products] >> gen_orders >> end

