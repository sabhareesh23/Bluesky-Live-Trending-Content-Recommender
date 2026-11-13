"""
Step 9: "Hello Spark" streaming job.

Reads posts from Kafka 'raw_posts' and, every 10 seconds, prints
how many posts arrived per language in 10-second windows.
Goal: prove Spark can read our live Kafka stream.

It runs inside the 'spark' Docker container. See the output with:
    docker logs -f spark
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window
from pyspark.sql.types import (ArrayType, LongType, StringType, StructField,
                               StructType)

KAFKA_SERVER = os.getenv("KAFKA_SERVER", "kafka:29092")

spark = (SparkSession.builder
         .appName("hello-stream")
         .config("spark.sql.shuffle.partitions", "6")       # default 200 is too many for our small stream
         .config("spark.ui.showConsoleProgress", "false")   # no progress bars in the logs
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")      # hide Spark's very chatty logs

# 1) Describe what one post looks like (must match producer.py)
post_schema = StructType([
    StructField("post_id", StringType()),
    StructField("author", StringType()),
    StructField("event_time_us", LongType()),
    StructField("created_at", StringType()),
    StructField("lang", StringType()),
    StructField("text", StringType()),
    StructField("hashtags", ArrayType(StringType())),
    StructField("ingest_time_ms", LongType()),
])

# 2) Read the Kafka topic as a never-ending table
raw = (spark.readStream
       .format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_SERVER)
       .option("subscribe", "raw_posts")
       .option("startingOffsets", "latest")   # only new posts
       .load())

# 3) Kafka gives bytes -> turn them into columns
posts = (raw
         .select(from_json(col("value").cast("string"), post_schema).alias("p"))
         .select("p.*")
         .where(col("post_id").isNotNull())    # ignore old test messages
         # use OUR clock (ingest time) as the event time -> avoids clock skew
         .withColumn("event_time", (col("ingest_time_ms") / 1000).cast("timestamp")))

# 4) Count posts per language in 10-second windows
counts = (posts
          .withWatermark("event_time", "30 seconds")   # wait max 30 s for late posts
          .groupBy(window("event_time", "10 seconds"), col("lang"))
          .count()
          .orderBy(col("window").desc(), col("count").desc()))

# 5) Print the result to the console every 10 seconds
query = (counts.writeStream
         .outputMode("complete")
         .format("console")
         .option("truncate", False)
         .option("numRows", 10)
         .trigger(processingTime="10 seconds")
         .start())

query.awaitTermination()
