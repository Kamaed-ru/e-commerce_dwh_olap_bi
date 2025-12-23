import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType,
    BooleanType, DateType
)



# ---------- args ----------
if len(sys.argv) < 2:
    raise ValueError("ingestion_date (YYYY-MM-DD) is required")

ingestion_date = sys.argv[1]

bucket = os.environ["S3_BUCKET"]
endpoint = os.environ["S3_ENDPOINT_URL"]
access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]


spark = SparkSession.builder.appName("stg_customers").getOrCreate()

hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", endpoint)
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


RAW_PATH = f"s3a://{bucket}/raw/customers/date={ingestion_date}/customers.json.gz"
STG_PATH = f"s3a://{bucket}/stg/customers/ingestion_date={ingestion_date}"


spark.sparkContext.setLogLevel("WARN")

# ---------- schema ----------
schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city_id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("registration_date", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("deleted", BooleanType(), True),
])

# ---------- read RAW ----------
df_raw = (
    spark.read
    .schema(schema)
    .json(RAW_PATH)
)

# ---------- transform ----------
df_stg = (
    df_raw
    .withColumn("registration_date", to_date(col("registration_date")))
    .withColumn("updated_at", to_date(col("updated_at")))
    .withColumn("ingestion_date", lit(ingestion_date).cast(DateType()))
)

# ---------- write STG ----------
(
    df_stg
    .repartition(1)  # 👈 временно, для контроля файлов
    .write
    .mode("overwrite")
    .parquet(STG_PATH)
)

spark.stop()
