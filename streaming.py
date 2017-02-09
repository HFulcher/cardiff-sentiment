import json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from textblob import TextBlob
from textwrap import TextWrapper
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch import client
import configparser
import sys

# import twitter keys and tokens
#from config import *
# create instance of elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}], http_auth=('elastic', 'changeme'))
ind = client.IndicesClient(es)

count = 0

class TweetStreamListener(StreamListener):
    print("Starting Stream")
    # on success
    def on_data(self, data):
        #decode json
        try:
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
            es.index(index="twitter-time-test",
                     doc_type="twitter_twp_test",
                     body={"author": dict_data["user"]["screen_name"],
                           "timestamp": timestamp,
                           "message": dict_data["text"],
                           "polarity": tweet.sentiment.polarity,
                           "subjectivity": tweet.sentiment.subjectivity,
                           "sentiment": sentiment})

            tweet_count()

            store_size = ind.stats()["indices"]["twitter-time-test"]["primaries"]["store"]["size_in_bytes"]

            if store_size < 2000000000:
                return True
            else:
                print("Max amount reached, stopping stream")
                return False

        except KeyboardInterrupt:
            print("Manully stopping stream")
            return False

        except Exception as e:
            print(e)
            pass

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

    # search twitter for "congress" keyword
    stream.filter(track=['obama'])
    print("Ending Stream")
