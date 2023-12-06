import os
import shutil
import json
from tweety import Twitter
from tweety.types.twDataTypes import Tweet, SelfThread
from kafka import KafkaProducer
import argparse
from datetime import datetime
import time

def save_tweet(tweet, save_path, producer):
    with open(os.path.join(save_path, '{}.json'.format(tweet.id)), 'w') as f:
        json.dump(tweet, f, default=str, ensure_ascii=False)

    if producer is not None:
        with open(os.path.join(save_path, '{}.json'.format(tweet.id)), 'r') as f:
            future = producer.send(topic, json.dumps(json.load(f), ensure_ascii=False).encode('utf-8'))
            print(future.get(timeout=10))

def crawl(app, username, pages, wait_time, output_dir, producer=None, topic=None):
    try:
        user = app.get_user_info(username)
        print(user)
    except:
        print("User {} not found!!!".format(username))
        return 0
    
    start = datetime.now().timestamp()
    try:
        print('Start scraping...{}.'.format(username))
        tweets_list = app.get_tweets(username, pages, wait_time=wait_time)

        print("Scraping completed, saving...")
        os.mkdir(os.path.join(output_dir, username))
        for tweets in tweets_list:
            if type(tweets) is Tweet:
                save_tweet(tweets, os.path.join(output_dir, username), producer)
            elif type(tweets) is SelfThread:
                for tweet in tweets:
                    save_tweet(tweet, os.path.join(output_dir, username), producer)
        
        end = datetime.now().timestamp()
        print('{}, Done in {} second!'.format(username, end - start))
        return 1
    except:
        print('Something went fucked!!!')
        return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default='./config.json', help="path to JSON config file")
    parser.add_argument("--output", type=str, help="path to JSON output directory")
    parser.add_argument("--kafka", type=str, nargs=2, default='null', help="import to kafka server IP:PORT TOPIC_NAME")
    args = parser.parse_args()

    with open(args.config, 'r') as cff:
        config = json.load(cff)

    if os.path.exists(args.output):
        shutil.rmtree(args.output)
        os.mkdir(args.output)
    else:
        os.mkdir(args.output)

    producer = None
    topic = None
    if args.kafka != 'null':
        print('Connecting to kafka server...')
        producer = KafkaProducer(bootstrap_servers = [args.kafka[0]])
        print('Kafka connected!!!')
        topic = args.kafka[1]
        print('Kafka server: {}'.format(args.kafka[0]))
        print('Kafka topic: {}'.format(args.kafka[1]))

    twitter_username = config['USERNAME']
    twitter_password = config['PASSWORD']

    app = Twitter("session")
    app.sign_in(twitter_username, twitter_password)

    with open(config['PATH2USERNAMELIST']) as f:
        usernames = f.read().strip().split('\n')
        print(usernames)
        for username in usernames:
            for i in range(3):
                status = crawl(app, username, config['PAGES'], config['WAITTIME'], args.output, producer, topic)
                if status == 1:
                    break
                else:
                    print('Waitting...')
                    time.sleep(60)
                    print('Trying again, {} times left.'.format(2 - i))
    
    if producer is not None:
        producer.close()
