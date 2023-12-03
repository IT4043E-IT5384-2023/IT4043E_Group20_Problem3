from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('group_20_problem3_raw_test', bootstrap_servers = ['34.142.194.212:29092'])

for msg in consumer:
    print(msg.value.decode('utf-8'))

