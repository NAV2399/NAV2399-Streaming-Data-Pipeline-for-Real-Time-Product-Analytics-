from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, window
from pyspark.sql.types import StructType, StringType, TimestampType

# Spark session
spark = SparkSession.builder \
    .appName("ProductAnalyticsStreaming") \
    .getOrCreate()

# Kafka stream config
schema = StructType() \
    .add("event_id", StringType()) \
    .add("user_id", StringType()) \
    .add("timestamp", StringType()) \
    .add("action", StringType()) \
    .add("product", StringType())

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "product_events") \
    .load()

df = df_raw.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", to_timestamp("timestamp"))

# Write to S3 or local path as Parquet
query = df.writeStream \
    .format("parquet") \
    .option("path", "output/streaming_data/") \
    .option("checkpointLocation", "output/checkpoints/") \
    .partitionBy("action") \
    .outputMode("append") \
    .start()

query.awaitTermination()
