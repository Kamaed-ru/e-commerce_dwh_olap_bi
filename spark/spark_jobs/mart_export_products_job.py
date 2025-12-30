import os, sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

if len(sys.argv) < 2:
    raise ValueError("ingestion_date is required")

p_date = sys.argv[1]


gp_url  = os.environ["GP_JDBC_URL"]
gp_user = os.environ["GP_USER"]
gp_pass = os.environ["GP_PASSWORD"]

bucket = os.environ["S3_BUCKET"]
endpoint = os.environ["S3_ENDPOINT_URL"]
access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

spark = SparkSession.builder.appName("export_customers_denorm").getOrCreate()


hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", endpoint)
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


spark.sparkContext.setLogLevel("WARN")

# Читаем products из GP
df_products = spark.read \
    .format("jdbc") \
    .option("url", gp_url) \
    .option("user", gp_user) \
    .option("password", gp_pass) \
    .option("dbtable", "core.products") \
    .load()

# Пишем в S3 (overwrite по дате запуска, как snapshot справочника)
OUT = f"s3a://{bucket}/mart/dim/products"
(
    df_products
    .repartition(1)
    .write
    .mode("overwrite")
    .parquet(OUT)
)

spark.stop()
