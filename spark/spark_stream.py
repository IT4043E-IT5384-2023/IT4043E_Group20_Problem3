# import findspark
# findspark.init()
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" pyspark-shell'
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType, IntegerType, BooleanType, TimestampType, BinaryType
from pyspark.sql.functions import col, array, struct, from_json, to_json, first, max, avg, explode, collect_list, collect_set, current_timestamp
import argparse
import time
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('master', type=str, help='spark server')
    parser.add_argument('name', type=str, help='spark app name')
    parser.add_argument('--kafka_in', type=str, nargs=2, default='null', help='listen from kafka server IP:PORT TOPIC_NAME')
    parser.add_argument('--kafka_out', type=str, nargs=2, default='null', help='listen from kafka server IP:PORT TOPIC_NAME')
    args = parser.parse_args()
    
    # spark = SparkSession.builder.appName('stream').master("spark://34.142.194.212:7077").getOrCreate()
    
    spark = SparkSession.builder.appName(args.name).master(args.master).getOrCreate()
    
    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', args.kafka_in[0]) \
        .option('subscribe', args.kafka_in[1]) \
        .option('startingOffsets', 'latest') \
        .load()
    
    in_schema = StructType([
        StructField('original_tweet', StringType()),
        StructField('id', StringType()),
        StructField('created_on', TimestampType()),
        StructField('date', TimestampType()),
        StructField('author', StructType([
            StructField('original_user', StringType()),
            StructField('id', StringType()),
            StructField('rest_id', IntegerType()),
            StructField('created_at', TimestampType()),
            StructField('date', TimestampType()),
            StructField('entities', StringType()),
            StructField('description', StringType()),
            StructField('bio', StringType()),
            StructField('fast_followers_count', IntegerType()),
            StructField('favourites_count', IntegerType()),
            StructField('followers_count', IntegerType()),
            StructField('friends_count', IntegerType()),
            StructField('has_custom_timelines', BooleanType()),
            StructField('is_translator', StringType()),
            StructField('listed_count', IntegerType()),
            StructField('location', StringType()),
            StructField('media_count', IntegerType()),
            StructField('name', StringType()),
            StructField('normal_followers_count', IntegerType()),
            StructField('profile_banner_url', StringType()),
            StructField('profile_image_url_https', StringType()),
            StructField('profile_interstitial_type', StringType()),
            StructField('protected', BooleanType()),
            StructField('screen_name', StringType()),
            StructField('username', StringType()),
            StructField('statuses_count', IntegerType()),
            StructField('translator_type', StringType()),
            StructField('verified', BooleanType()),
            StructField('can_dm', BooleanType()),
            StructField('following', BooleanType()),
            StructField('community_role', StringType()),
            StructField('notifications_enabled', BooleanType()),
            StructField('notifications', BooleanType()),
            StructField('possibly_sensitive', StringType()),
            StructField('pinned_tweets', StringType()),
            StructField('profile_url', StringType())
        ])),
        StructField('is_retweet', BooleanType()),
        StructField('retweeted_tweet', StringType()),
        StructField('rich_text', StringType()),
        StructField('text', StringType()),
        StructField('tweet_body', StringType()),
        StructField('is_quoted', BooleanType()),
        StructField('quoted_tweet', StringType()),
        StructField('is_reply', BooleanType()),
        StructField('is_sensitive', BooleanType()),
        StructField('reply_count', IntegerType()),
        StructField('quote_count', IntegerType()),
        StructField('replied_to', StringType()),
        StructField('bookmark_count', IntegerType()),
        StructField('vibe', StringType()),
        StructField('views', IntegerType()),
        StructField('language', StringType()),
        StructField('likes', IntegerType()),
        StructField('place', StringType()),
        StructField('retweet_count', IntegerType()),
        StructField('source', StringType()),
        StructField('audio_space_id', StringType()),
        StructField('voice_info', StringType()),
        StructField('media', StringType()),
        StructField('pool', StringType()),
        StructField('user_mentions', StringType()),
        StructField('urls', StringType()),
        StructField('hashtags', StringType()),
        StructField('symbols', StringType()),
        StructField('community_note', StringType()),
        StructField('community', StringType()),
        StructField('url', StringType()),
        StructField('threads', StringType()),
        StructField('comments', StringType())
    ])
    
    df_json = df.select(from_json(col('value').cast(StringType()), in_schema).alias('json'))
    
    # df_filtered = df_jsons.select(explode(col('jsons')).alias('json'))
    df_filtered = df_json.select(
        col('json.author.id'),
        col('json.author.username').alias('username'),
        col('json.author.created_at').alias('created_at'),
        col('json.author.favourites_count').cast(FloatType()).alias('favourites_count'),
        col('json.author.followers_count').cast(FloatType()).alias('followers_count'),
        col('json.author.friends_count').cast(FloatType()).alias('friends_count'),
        col('json.id').alias('tweet_id'),
        (col('json.created_on').cast(FloatType())/(3600*24)).alias('created_on'),
        col('json.is_retweet').cast(FloatType()).alias('is_retweet'),
        col('json.text'),
        col('json.reply_count').cast(FloatType()).alias('reply_count'),
        col('json.quote_count').cast(FloatType()).alias('quote_count'),
        col('json.bookmark_count').cast(FloatType()).alias('bookmark_count'),
        col('json.likes').cast(FloatType()).alias('likes'),
        col('json.retweet_count').cast(FloatType()).alias('retweet_count')
    ).fillna(0.0)

    print('Data Schema:')
    df_filtered.printSchema()
    
    df_out = df_filtered.select(to_json(struct(*[col(col_name) for col_name in df_filtered.columns])).cast(BinaryType()).alias('value'))
    
    query1 = df_filtered.writeStream \
        .format('console') \
        .start()
    
    query2 = df_out.writeStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', args.kafka_out[0]) \
        .option('topic', args.kafka_out[1]) \
        .option('checkpointLocation', './checkpoint') \
        .start()
    
    spark.streams.awaitAnyTermination()
