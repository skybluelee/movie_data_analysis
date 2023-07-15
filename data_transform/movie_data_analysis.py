# zeppelin file code
%pyspark

# HDFS에서 리뷰 데이터 read
df1 = spark.read \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .csv("hdfs://spark-master-01:9000/skybluelee/movie/part-01.csv") 

df2 = spark.read \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .csv("hdfs://spark-master-01:9000/skybluelee/movie/part-02.csv")
            
df3 = spark.read \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .csv("hdfs://spark-master-01:9000/skybluelee/movie/part-03.csv")
            
df4 = spark.read \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .csv("hdfs://spark-master-01:9000/skybluelee/movie/part-04.csv")
            
df5 = spark.read \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .csv("hdfs://spark-master-01:9000/skybluelee/movie/part-05.csv")
            
df6 = spark.read \
           .option("header", "true") \
           .option("inferSchema", "false") \
           .csv("hdfs://spark-master-01:9000/skybluelee/movie/part-06.csv")  

# HDFS 환경에서는 각 value값의 type이 제대로 지정되어 있지만
# Spark으로 read하는 순간 모든 type이 str이 되므로 type을 변경
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType

df1 = df1.withColumn('rating', col('rating').cast(IntegerType()))\
         .withColumn('spoiler_tag', col('spoiler_tag').cast(IntegerType()))\
         .withColumn("review_date",to_timestamp("review_date"))
         
df2 = df2.withColumn('rating', col('rating').cast(IntegerType()))\
         .withColumn('spoiler_tag', col('spoiler_tag').cast(IntegerType()))\
         .withColumn("review_date",to_timestamp("review_date"))
         
df3 = df3.withColumn('rating', col('rating').cast(IntegerType()))\
         .withColumn('spoiler_tag', col('spoiler_tag').cast(IntegerType()))\
         .withColumn("review_date",to_timestamp("review_date"))
         
df4 = df4.withColumn('rating', col('rating').cast(IntegerType()))\
         .withColumn('spoiler_tag', col('spoiler_tag').cast(IntegerType()))\
         .withColumn("review_date",to_timestamp("review_date"))
         
df5 = df5.withColumn('rating', col('rating').cast(IntegerType()))\
         .withColumn('spoiler_tag', col('spoiler_tag').cast(IntegerType()))\
         .withColumn("review_date",to_timestamp("review_date"))
         
df6 = df6.withColumn('rating', col('rating').cast(IntegerType()))\
         .withColumn('spoiler_tag', col('spoiler_tag').cast(IntegerType()))\
         .withColumn("review_date",to_timestamp("review_date"))         
         
# Spark SQL을 사용하기 위해 해당 dataframe을 임시 table로 생성
df1.createOrReplaceTempView("part_01")
df2.createOrReplaceTempView("part_02")
df3.createOrReplaceTempView("part_03")
df4.createOrReplaceTempView("part_04")
df5.createOrReplaceTempView("part_05")
df6.createOrReplaceTempView("part_06") 

# 데이터 확인
df1.printSchema()

root
 |-- review_id: string (nullable = true)
 |-- reviewer: string (nullable = true)
 |-- movie: string (nullable = true)
 |-- rating: integer (nullable = true)
 |-- review_summary: string (nullable = true)
 |-- review_date: timestamp (nullable = true)
 |-- spoiler_tag: integer (nullable = true)
 |-- review_detail: string (nullable = true)
 |-- helpful: string (nullable = true)

# 2019~2021년에 해당하는 데이터를 해당 년도의 데이터만 존재하도록 설정
df_01 = spark.sql("""
                     SELECT  *, YEAR(review_date) AS YEAR, MONTH(review_date) AS MONTH,
                             CASE WHEN DAY(review_date) BETWEEN 1 AND 10 THEN '1_10'
                                  WHEN DAY(review_date) BETWEEN 11 AND 20 THEN '11_20'
                                  ELSE '21_31' END AS DAY
                     FROM    part_01
                     WHERE   YEAR(review_date) IN (2019, 2020, 2021)
                 """)
                      
df_02 = spark.sql("""
                     SELECT  *, YEAR(review_date) AS YEAR, MONTH(review_date) AS MONTH,
                             CASE WHEN DAY(review_date) BETWEEN 1 AND 10 THEN '1_10'
                                  WHEN DAY(review_date) BETWEEN 11 AND 20 THEN '11_20'
                                  ELSE '21_31' END AS DAY
                     FROM    part_02
                     WHERE   YEAR(review_date) IN (2019, 2020, 2021)
                 """)
                      
df_03 = spark.sql("""
                     SELECT  *, YEAR(review_date) AS YEAR, MONTH(review_date) AS MONTH,
                             CASE WHEN DAY(review_date) BETWEEN 1 AND 10 THEN '1_10'
                                  WHEN DAY(review_date) BETWEEN 11 AND 20 THEN '11_20'
                                  ELSE '21_31' END AS DAY
                     FROM    part_03
                     WHERE   YEAR(review_date) IN (2019, 2020, 2021)
                 """)  
                      
df_04 = spark.sql("""
                     SELECT  *, YEAR(review_date) AS YEAR, MONTH(review_date) AS MONTH,
                             CASE WHEN DAY(review_date) BETWEEN 1 AND 10 THEN '1_10'
                                  WHEN DAY(review_date) BETWEEN 11 AND 20 THEN '11_20'
                                  ELSE '21_31' END AS DAY
                     FROM    part_04
                     WHERE   YEAR(review_date) IN (2019, 2020, 2021)
                 """)           

df_total = df_01                    
df_total = df_total.union(df_02) 
df_total = df_total.union(df_03) 
df_total = df_total.union(df_04)

# 데이터를 파티셔닝하여 HDFS에 업로드
df_total.coalesce(1).write \
                    .option("header",True) \
                    .partitionBy("YEAR", "MONTH", "DAY") \
                    .mode("overwrite") \
                    .json("hdfs://spark-master-01:9000/skybluelee/movie-partitioned-json")