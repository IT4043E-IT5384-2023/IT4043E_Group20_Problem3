import os
import shutil
import json
from tweety import Twitter
from tweety.filters import SearchFilters
import csv
import argparse
from datetime import datetime
import time

def crawl(output_dir, app, username, pages, waittime):
    try:
        user = app.get_user_info(username)
        print(user)
    except:
        print("User {} not found!!!".format(username))
        return 1
    
    start = datetime.now().timestamp()
    try:
        print('Start scraping...{}.'.format(username))
        tweets = app.get_tweets(username, pages, wait_time=waittime)

        print("Scraping completed, saving...")
        with open(os.path.join(output_dir, '{}.json'.format(username)), 'w') as f:
            for tweet in tweets:
                json.dump(tweet.__dict__, f, default=str)
        
        end = datetime.now().timestamp()
        print('{}, Done in {} second!'.format(username, end - start))
        return 1
    except:
        print('Something went fucked!!!')
        return 0

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, help="path to JSON config file")
    parser.add_argument("--output", type=str, help="path to JSON output directory")
    args = parser.parse_args()

    with open(args.config, 'r') as cff:
        config = json.load(cff)

    if os.path.exists(args.output):
        shutil.rmtree(args.output)
        os.mkdir(args.output)
    else:
        os.mkdir(args.output)

    twitter_username = config['USERNAME']
    twitter_password = config['PASSWORD']

    app = Twitter("session")
    app.sign_in(twitter_username, twitter_password)

    with open(config['PATH2USERNAMELIST'], newline='') as f:
        rows = csv.DictReader(f)
        for row in rows:
            for i in range(3):
                status = crawl(args.output, app, row['username'], config['PAGES'], config['WAITTIME'])
                if status == 1:
                    break
                else:
                    print('Restarting session...')
                    time.sleep(30)
                    app = Twitter("session")
                    app.sign_in(twitter_username, twitter_password)

