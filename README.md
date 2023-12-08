# IT4043E_Group20_Problem3

1. Install the requirement:
```
pip install -r requirements.txt
```

2. Fill config.json file:
```
"USERNAME" : twitter username
"PASSWORD" : twitter password

"PATH2USERNAMELIST" : path to a .txt file storing list of usernames with newline seperator.

"PAGES" : number of pages crawled for each user.
"WAITIME" : number of seconds to wait between multiple requests.
```

3.Run the crawler:
```
python twitter_crawler.py --config <PATH_TO_CONFIG_FILE> --output <PATH_TO_OUTPUT_FOLDER> --kafka <KAFKA_SERVER_IP:PORT TOPIC> 
```
Ex:
```
python twitter_crawler.py --config ./config.json --output ./data_raw/ --kafka 69.69.69.69 topic_0
```
