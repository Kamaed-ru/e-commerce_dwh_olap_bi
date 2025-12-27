import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, DateType

if len(sys.argv) < 2:
    raise ValueError("ingestion_date is required")
ingestion_date = sys.argv[1]

bucket = os.environ["S3_BUCKET"]
endpoint = os.environ["S3_ENDPOINT_URL"]
access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

spark = SparkSession.builder.appName("stg_categories").getOrCreate()
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", endpoint)
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("category_id", IntegerType(), False),
    StructField("category_name", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("deleted", BooleanType(), True),
])

RAW_PATH = f"s3a://{bucket}/raw/categories/categories.json.gz"
STG_PATH = f"s3a://{bucket}/stg/categories"

df_raw = spark.read.schema(schema).json(RAW_PATH)

df_stg = df_raw.withColumn("created_at", to_date(col("created_at")))

(
    df_stg.repartition(1)
          .write.mode("overwrite")
          .parquet(STG_PATH)
)

spark.stop()
