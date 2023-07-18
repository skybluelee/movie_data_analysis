from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'tweet',
    bootstrap_servers = [
        "spark-worker-01:9092","spark-worker-02:9092","spark-worker-03:9092"
    ],
    auto_offset_reset='latest',
    group_id='mygroup',
    enable_auto_commit=True,
    value_deserializer = lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=10000 # n초간 메시지가 들어오지 않으면 종료
)

for msg in consumer:
    print(f'topic={msg.topic}, partition={msg.partition}, offset={msg.offset}, key={msg.key}, value={msg.value}')