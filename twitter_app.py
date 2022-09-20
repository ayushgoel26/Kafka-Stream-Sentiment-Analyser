import tweepy
import logging
from kafka import KafkaProducer
import config
import json


class KafkaConfig:
    def __int__(self):
        self.producer = KafkaProducer(bootstrap_servers='localhost:9092', api_version=(2, 0, 2))

    def get_producer(self):
        return self.producer


# class TwitterAuth:
#     def __init__(self):
#         self.auth = None
#
#     def authenticateTwitterApp(self):
#         self.auth = tweepy.OAuthHandler(config.ConsumerKey, config.ConsumerKeySecret)
#         self.auth.set_access_token(config.AccessToken, config.AccessTokenSecret)
#         # api = tweepy.API(self.auth, wait_on_rate_limit=True)
#         return self.auth


class TweetListener(tweepy.Stream):
    def __int__(self):
        self.kafka_config = KafkaConfig()

    def on_data(self, raw_data):
        print(raw_data)
        self.kafka_config.get_producer().send(config.TopicName, value=raw_data)
        return True

    @staticmethod
    def on_error(self, status):
        print(status)
        if status == 420:
            return False

    def start_streaming_tweets(self):
        self.filter(track=[config.SearchTerm], languages=['en'])


if __name__ == "__main__":
    logging.basicConfig(filename='twitter_apiv1_log.log', level=logging.DEBUG)
    twitter_streamer = TweetListener(config.APIKey, config.APIKeySecret, config.AccessTokenNew, config.AccessTokenSecretNew)
    twitter_streamer.start_streaming_tweets()
