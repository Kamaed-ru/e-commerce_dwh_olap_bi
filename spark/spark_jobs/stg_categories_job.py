import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, to_date
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
# читаем RAW
df_raw = spark.read.schema(schema).json(f"s3a://{bucket}/raw/categories/categories.json.gz")

# преобразуем дату, но НЕ включаем в хеш
df_new = df_raw.withColumn("created_at", to_date(col("created_at")))

# хешируем только значимые поля
df_new_hashed = df_new.withColumn(
    "row_hash",
    sha2(
        concat_ws("||", col("category_id"), col("category_name"), col("deleted")),
        256
    )
)

stg_path = f"s3a://{bucket}/stg/categories"
layer_exists = os.system(f"hadoop fs -test -e {stg_path}") == 0

if layer_exists:
    df_old = spark.read.parquet(stg_path)

    # если в старом слое нет хеша — создадим для сравнения
    if "row_hash" not in df_old.columns:
        df_old = df_old.withColumn(
            "row_hash",
            sha2(concat_ws("||", col("category_id"), col("category_name"), col("deleted")), 256)
        )

    old_hashes = df_old.select("row_hash").collect()
    new_hashes = df_new_hashed.select("row_hash").collect()

    if old_hashes == new_hashes:
        print("No changes detected — categories will not be overwritten")
        spark.stop()
        sys.exit(0)

# сохраняем новый STG, если изменения есть
(
    df_new_hashed.drop("row_hash")
                .repartition(1)
                .write.mode("overwrite")
                .parquet(stg_path)
)

print("Changes detected — categories updated in STG layer")
spark.stop()
