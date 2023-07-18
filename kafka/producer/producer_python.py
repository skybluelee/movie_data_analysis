from kafka import KafkaProducer
from json import dumps
import time
import json

producer = KafkaProducer(
    acks = 0,
    bootstrap_servers = [
        "spark-worker-01:9092","spark-worker-02:9092","spark-worker-03:9092"
    ],
    value_serializer = lambda x: dumps(x).encode('utf-8')
)
json_data = []
with open('/skybluelee/movie-partitioned-json/YEAR=2019/MONTH=1/DAY=1_10/part-00000-15cb06cb-1236-40ee-aaf6-df9354d70a74.c000.json', 'r') as f:
    for line in f:
        json_data.append(json.loads(line))

topic = 'tweet'
start = time.time()
for data in json_data:
    producer.send(topic, value=data)
    producer.flush()
print('elapsed: ', time.time()-start)