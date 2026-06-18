from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet(
    "data/bronze/record_type=user_profile"
)

df.printSchema()
df.show(5, truncate=False)