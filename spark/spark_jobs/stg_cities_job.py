import sys, os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws, to_date
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType

if len(sys.argv) < 2:
    raise ValueError("ingestion_date is required")
ingestion_date = sys.argv[1]

bucket = os.environ["S3_BUCKET"]
endpoint = os.environ["S3_ENDPOINT_URL"]
access_key = os.environ["AWS_ACCESS_KEY_ID"]
secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]

spark = SparkSession.builder.appName("stg_cities").getOrCreate()
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", endpoint)
hadoop_conf.set("fs.s3a.access.key", access_key)
hadoop_conf.set("fs.s3a.secret.key", secret_key)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark.sparkContext.setLogLevel("WARN")

# читаем RAW слой
schema = StructType([
    StructField("city_id", IntegerType(), False),
    StructField("city_name", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("deleted", BooleanType(), True),
])

df_raw = spark.read.schema(schema).json(f"s3a://{bucket}/raw/cities/cities.json.gz")

# приводим дату, но не используем в хеше
df_new = df_raw.withColumn("created_at", to_date(col("created_at")))

# считаем хеш только по значимым полям БЕЗ created_at
df_new_hashed = df_new.withColumn(
    "row_hash",
    sha2(
        concat_ws("||", col("city_id"), col("city_name"), col("country"), col("deleted")),
        256
    )
)

# проверяем существование STG слоя
stg_path = f"s3a://{bucket}/stg/cities"
table_exists = os.system(f"hadoop fs -test -e {stg_path}") == 0

if table_exists:
    df_old = spark.read.parquet(stg_path)

    # если в старом слое нет хеша (первый запуск) — создадим его для сравнения
    if "row_hash" not in df_old.columns:
        df_old = df_old.withColumn(
            "row_hash",
            sha2(concat_ws("||", col("city_id"), col("city_name"), col("country"), col("deleted")), 256)
        )

    old_hash = df_old.select("row_hash").collect()
    new_hash = df_new_hashed.select("row_hash").collect()

    # если хеши одинаковые → выходим без перезаписи
    if old_hash == new_hash:
        print("No changes detected — cities will not be overwritten")
        spark.stop()
        sys.exit(0)

# если изменения есть → сохраняем новый STG слой
(
    df_new_hashed.drop("row_hash")
                .repartition(1)
                .write.mode("overwrite")
                .parquet(stg_path)
)

print("Changes detected — cities updated in STG layer")
spark.stop()
