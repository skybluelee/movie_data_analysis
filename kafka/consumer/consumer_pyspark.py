# zeppelin 환경에서 실행
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

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

# df값을 string으로 변환
df_stream_value = df_consumer \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(col("value"))

# 데이터를 특정 주기마다 hdfs에 업로드
query_df_stream_tweet_hdfs_text = df_stream_value \
    .writeStream \
    .trigger(processingTime='5 seconds') \
    .outputMode("append") \
    .format("text") \
    .option("path", "hdfs://spark-master-01:9000/skybluelee/data/tweet_from_structured_streaming") \
    .option("checkpointLocation", "hdfs://spark-master-01:9000/checkpoint/structured_streaming/tweet_hdfs_text") \
    .queryName("query_df_stream_tweet_hdfs_text") \
    .start()