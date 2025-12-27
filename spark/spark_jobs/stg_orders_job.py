import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DateType,  LongType
)

if len(sys.argv) < 2:
    raise ValueError("ingestion_date is required")

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

RAW_PATH = f"s3a://{bucket}/raw/orders/date={ingestion_date}/orders.json.gz"
STG_PATH = f"s3a://{bucket}/stg/orders/ingestion_date={ingestion_date}"

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id",  LongType(), False),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("order_status", StringType(), True),
])

df = (
    spark.read
    .schema(schema)
    .json(RAW_PATH)
    .withColumn("order_date", to_date(col("order_date")))
    .withColumn("ingestion_date", lit(ingestion_date).cast(DateType()))
)

(
    df
    .repartition(1)
    .write
    .mode("overwrite")
    .parquet(STG_PATH)
)

spark.stop()
