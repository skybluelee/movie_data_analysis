# zeppelin 환경에서 실행
%pyspark
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# data using in producer
df = spark.read \
          .option("header", "true") \
          .option("inferSchema", "false") \
          .json("hdfs://spark-master-01:9000/skybluelee/movie-partitioned-json/YEAR=2019/MONTH=1/DAY=11_20/*.json")

# Define Kafka broker and topic
brokers = "spark-worker-01:9092,spark-worker-02:9092,spark-worker-03:9092"
topic = "tweet"

# write data to Kafka topic as a streaming source
df = spark \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", brokers) \
    .option("topic", topic) \
    .start()

# df.stop()