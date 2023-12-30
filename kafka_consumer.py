import os
import shutil
from kafka import KafkaConsumer
import argparse
import json

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("server", type=str, help="Kafka server IP:PORT")
    parser.add_argument("topic", type=str, help="Kafka topic")
    args = parser.parse_args()

    consumer = KafkaConsumer(args.topic, bootstrap_servers = [args.server], auto_offset_reset='latest')

    if os.path.exists('processed_data'):
        shutil.rmtree('processed_data')
    os.mkdir('processed_data')

    for msg in consumer:
        print(msg.value.decode('utf-8'))
        text = json.loads(msg.value.decode('utf-8'))
        if not os.path.exists('processed_data/{}'.format(text['id'])):
            os.mkdir('processed_data/{}'.format(text['id']))
        with open('processed_data/{}/{}.json'.format(text['id'], text['tweet_id']), 'w', encoding='utf-8') as f:
            json.dump(text, f, default=str, ensure_ascii=False)

    consumer.close()
