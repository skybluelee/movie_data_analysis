%pyspark
from pyspark.sql import SparkSession
from pymongo.mongo_client import MongoClient
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType

# MongoDB uri & setting
uri = ""

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Define Kafka broker and topic
brokers = "spark-worker-01:9092,spark-worker-02:9092,spark-worker-03:9092"
topic = "tweet"

# Read data from Kafka topic as a streaming source
df_consumer = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("subscribe", topic) \
    .load()
    
df_stream_value = df_consumer \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #.select(col("value")) 

# 스키마 정의
schema = StructType([
    StructField("review_id", StringType(), True),
    StructField("reviewer", StringType(), True),
    StructField("movie", StringType(), True),
    StructField("rating", IntegerType(), True),
    StructField("review_summary", StringType(), True),
    StructField("review_date", StringType(), True),
    StructField("spoiler_tag", IntegerType(), True),
    StructField("review_detail", StringType(), True),
    StructField("helpful", StringType(), True),
])

# 기다리면 잘됨
df_parsed = df_stream_value.withColumn("value_json", from_json(col("value"), schema))
df_json = df_parsed.select("value_json.*")
    
df_json.writeStream \
              .format("mongodb") \
              .trigger(processingTime='5 seconds') \
              .option("spark.mongodb.connection.uri", uri) \
              .option("spark.mongodb.database", "movie") \
              .option("spark.mongodb.collection", "test7") \
              .outputMode("append") \
              .option("checkpointLocation", "hdfs://spark-master-01:9000/checkpoint/movie/test") \
              .start()