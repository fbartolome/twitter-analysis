import tweepy
from kafka import KafkaProducer
from tweepy import OAuthHandler
from tweepy import API
from tweepy import Stream
from tweepy.streaming import StreamListener
from http.client import IncompleteRead

from datetime import datetime, timedelta
from time import sleep
import json
import sys


class TwitterAuth():
    """SET UP TWITTER AUTHENTICATION"""

    def authenticateTwitterApp(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)

        return auth

class ListenerTS(StreamListener):

    def __init__(self, producer):
        self.producer = producer

    def on_data(self, raw_data):
        as_json = json.loads(raw_data)
        relevant_info = {
            "created_at":as_json["created_at"],
            "user_id":as_json["user"]["id"],
            "tweet_id":as_json["id"],
            "tweet":as_json["text"],
        }
        print(as_json["text"])
        print('-------------------')
        self.producer.send(topic, relevant_info)
        return True

if __name__ == '__main__':
    # Initialization
    args = sys.argv

    _, brokers, topic, consumer_key, consumer_secret, access_token, access_token_secret, search = args

    producer = KafkaProducer(
        bootstrap_servers=brokers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token,access_token_secret)
    if API(auth).verify_credentials() == False:
        raise Exception("Invalid credentials!")
    
    while True:
        try:
            listener = ListenerTS(producer=producer) 
            stream = Stream(auth, listener)
            stream.filter(track=search.split(','),stall_warnings=True, locations=[-34.699053, -58.533558,-34.529614, -58.333900])
        except IncompleteRead:
            print("Incomplete read.")
            continue