# IT4043E_Group20_Problem3

##### Data Structure

```
root
 |-- id: string (nullable = true)
 |-- username: string (nullable = true)
 |-- created_at: timestamp (nullable = true)
 |-- description: string (nullable = true)
 |-- fast_followers_count: integer (nullable = true)
 |-- favourites_count: integer (nullable = true)
 |-- followers_count: integer (nullable = true)
 |-- friends_count: integer (nullable = true)
 |-- normal_followers_count: integer (nullable = true)
 |-- protected: boolean (nullable = true)
 |-- verified: boolean (nullable = true)
 |-- tweets: array (nullable = false)
 |    |-- element: struct (containsNull = false)
 |    |    |-- id: string (nullable = true)
 |    |    |-- created_on: timestamp (nullable = true)
 |    |    |-- text: string (nullable = true)
 |    |    |-- is_retweet: boolean (nullable = true)
 |    |    |-- is_sensitive: boolean (nullable = true)
 |    |    |-- reply_counts: integer (nullable = true)
 |    |    |-- quote_counts: integer (nullable = true)
 |    |    |-- bookmark_count: integer (nullable = true)
 |    |    |-- views: integer (nullable = true)
 |    |    |-- likes: integer (nullable = true)
 |    |    |-- retweet_counts: integer (nullable = true)
```

##### Code Execution
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

3. Run the spark scripts:
```
python spark_stream.py <SPARK_SERVER_IP> <NAME> \
    --kafka_in <KAFKA_SERVER_IP:PORT> <TOPIC> \
    --kafka_out <KAFKA_SERVER_IP:PORT> <TOPIC> \
```
Ex:
```
python spark_stream.py local[*] stream \
    --kafka_in 69.69.69.69 topic_0_in \
    --kafka_out 69.69.69.69 topic_0_out \
```
3. Run the kafka consumer:
```
python kafka_consumer.py <KAFKA_SERVER_IP:PORT> <TOPIC>
```
Ex:
```
python kafka_consumer.py  69.69.69.69 topic_0_out
```
5. Run the crawler:
```
python twitter_crawler.py
    --config <PATH_TO_CONFIG_FILE>
    --output <PATH_TO_OUTPUT_FOLDER>
    --kafka <KAFKA_SERVER_IP:PORT> <TOPIC> 
```
Ex:
```
python twitter_crawler.py --config ./config.json --output ./data_raw/ --kafka 69.69.69.69 topic_0_in
```
6. Prcocess data and write to elasticsearch
Load pretrained model and predict the account quality then import to elasticsearch for querying and visualization.