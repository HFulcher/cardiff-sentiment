import json
import pprint
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from textwrap import TextWrapper
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import client
import traceback
import configparser
import sys

# create instance of elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}], http_auth=('elastic', 'changeme'))

#Get instance of indexes to check file size
ind = client.IndicesClient(es)

#For counting tweets streamed
count = 0

class TweetStreamListener(StreamListener):
    print("Starting Stream")
    # on success
    def on_data(self, data):

        try:
            #decode json
            dict_data = json.loads(data)

            # pass tweet into TextBlob
            tweet = TextBlob(dict_data["text"])

            # output sentiment polarity
            #print(tweet.sentiment.polarity)

            # determine if sentiment is positive, negative, or neutral
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
            else:
                sentiment = "positive"

            # output sentiment
            #print(sentiment)
            timestamp = datetime.strptime(dict_data["created_at"].replace("+0000 ",""), "%a %b %d %H:%M:%S %Y").isoformat()

            # add text and sentiment info to elasticsearch
            es.index(index="twitter-cardiff_sentiment",
                     doc_type="twitter_twp",
                     body={"author": dict_data["user"]["screen_name"],
                           "fullname": dict_data["user"]["name"],
                           "description": dict_data["user"]["description"],
                           "location": dict_data["user"]["location"],
                           "timestamp": timestamp,
                           "message": dict_data["text"],
                           "polarity": tweet.sentiment.polarity,
                           "subjectivity": tweet.sentiment.subjectivity,
                           "sentiment": sentiment})

            #Increment tweet count and display tweet amount
            tweet_count()

            #Get size of index
            store_size = ind.stats()["indices"]["twitter-cardiff_sentiment"]["primaries"]["store"]["size_in_bytes"]

            #If index exceeds 2GB shut down, otherwise continue
            if store_size < 2000000000:
                return True
            else:
                print("Max amount reached, stopping stream")
                return False

        #Catches issues with TextBlob not being able to parse text
        except Exception as e:
            f = open("streaming_error_log", 'a')
            f.write('-'*60 + '\n')
            traceback.print_exc(file=f)
            f.close()

    # on failure
    def on_error(self, status):
        print(status)

def tweet_count():
    global count
    count = count + 1
    print("Tweets: {}".format(count), end="\r")

if __name__ == '__main__':
    config = configparser.ConfigParser()
    config.read("config.ini")

    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()

    consumer_key = config["DEFAULT"]["consumer_key"]
    consumer_secret = config["DEFAULT"]["consumer_secret"]
    access_token = config["DEFAULT"]["access_token"]
    access_token_secret = config["DEFAULT"]["access_token_secret"]

    # set twitter keys/tokens
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    # create instance of the tweepy stream
    stream = Stream(auth, listener)

    # search twitter for "cardiff" keyword
    stream.filter(track=['cardiff'])
    print("Ending Stream")
