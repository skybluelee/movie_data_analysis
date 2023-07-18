# zeppelin 환경에서 실행
%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, LongType
from pyspark.sql.functions import lit

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Define the schema for the CSV file
schema = StructType() \
    .add("helpful", StringType()) \
    .add("movie", StringType()) \
    .add("rating", LongType()) \
    .add("review_date", StringType()) \
    .add("review_detail", StringType()) \
    .add("review_id", StringType()) \
    .add("review_summary", StringType()) \
    .add("reviewer", StringType()) \
    .add("spoiler_tag", LongType())

# data using in producer
file_path = "hdfs://spark-master-01:9000/skybluelee/movie-partitioned-json/YEAR=2019/MONTH=1/DAY=11_20/*.json"
streaming_df = spark.readStream \
                    .format("json") \
                    .schema(schema) \
                    .load(file_path)
                    
streaming_df = streaming_df.withColumn("value", lit(""))  # Add an empty 'value' column if it doesn't exist                    

# Define Kafka broker and topic
brokers = "spark-worker-01:9092,spark-worker-02:9092,spark-worker-03:9092"
topic = "tweet"

# write data to Kafka topic as a streaming source
query = streaming_df \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("topic", topic) \
    .option("checkpointLocation", "hdfs://spark-master-01:9000/checkpoint/structured_streaming/tweet_hdfs_text2") \
    .start() \
    .awaitTermination()

query.stop()