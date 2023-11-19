# IT4043E_Group20_Problem3

1. Install the requirement:
```
pip install -r requirements.txt
```

2. Fill config.json file:
```
"USERNAME" : twitter username
"PASSWORD" : twitter password

"PATH2USERNAMELIST" : pasth to a csv file storing usernames with column name 'username'.

"PAGES" : number of pages crawled for each user.
"WAITIME" : number of seconds to wait between multiple requests.
```

3.Run the crawler:
```
python twitter_crawler.py --config <PATH_TO_CONFIG_FILE> --output <PATH_TO_OUTPUT_FOLDER>
```
Ex:
```
python twitter_crawler.py --config ./config.json --output ./data_raw/
```
