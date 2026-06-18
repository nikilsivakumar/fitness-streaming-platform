# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .master("local[*]")
#     .appName("parquet-test")
#     .getOrCreate()
# )

# df = spark.read.parquet(
#     r"data\bronze\record_type=wearable_event"
# )

# print(df.count())

# spark.stop()


# parquet_test2.py

# from pyspark.sql import SparkSession

# spark = (
#     SparkSession.builder
#     .master("local[*]")
#     .appName("test")
#     .config("spark.hadoop.io.native.lib.available", "false")
#     .getOrCreate()
# )

# df = spark.read.parquet(
#     r"data\bronze\record_type=wearable_event"
# )

# print(df.count())

# spark.stop()



#------------------------------------------

# from pyspark.sql import SparkSession

# spark = SparkSession.builder.master("local[*]").getOrCreate()

# df = spark.read.format("parquet").load(
#     "file:///C:/Users/ASUS/Documents/AWS Data Eng/Study/Project/fitness-streaming-platform/data/bronze/record_type=wearable_event"
# )

# print(df.count())

# spark.stop()


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet(
    r"data\bronze\record_type=wearable_event\year=2026\month=05\day=22\batch_20260522_112534_339883.parquet"
)

df.printSchema()
df.show(5, truncate=False)