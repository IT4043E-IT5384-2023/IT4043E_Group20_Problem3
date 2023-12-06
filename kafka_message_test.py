from kafka import KafkaConsumer
import json

consumer = KafkaConsumer('group_20_problem3_raw_test', bootstrap_servers = ['34.142.194.212:29092'], auto_offset_reset='earliest')

for msg in consumer:
    print(msg.value.decode('utf-8'))
    print(consumer.assignment())

consumer.close()
