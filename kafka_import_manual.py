import os
import glob
import json
from kafka import KafkaProducer
import argparse
import time

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("input_dir", type=str, help="path of input folder")
    parser.add_argument("server", type=str, help="Kafka server IP:PORT")
    parser.add_argument("topic", type=str, help="Kafka topic")
    args = parser.parse_args()

    print('Connecting to kafka server...')
    producer = KafkaProducer(bootstrap_servers = [args.server])
    print('Kafka connected!!!')
    topic = args.topic
    print('Kafka server: {}'.format(args.server))
    print('Kafka topic: {}'.format(args.topic))

    num_file = len(glob.glob(os.path.join(args.input_dir, '**/*.json'), recursive=True))
    for i, file in enumerate(glob.glob(os.path.join(args.input_dir, '**/*.json'), recursive=True)):
        print('Sending [{}/{}] {}'.format(i + 1, num_file, file))
        with open(file, 'r', encoding='utf-8') as f:
            future = producer.send(topic, json.dumps(json.load(f), ensure_ascii=False).encode('utf-8'))
            print(future.get(timeout=30))
        time.sleep(5)

    producer.close()