# Import libraries
import config
import logging
import json
import re
from tweepy import StreamingClient, StreamRule
from kafka import KafkaProducer
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.tuning import CrossValidatorModel

# Generate Kafka producer/ localhost and 9092 default ports
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

sc = SparkContext('local')
spark = SparkSession(sc)


def cleanTweet(tweet: str) -> str:
    tweet = re.sub(r'http\S+', '', str(tweet))
    tweet = re.sub(r'bit.ly/\S+', '', str(tweet))
    tweet = tweet.strip('[link]')

    # Remove users
    tweet = re.sub('(RT\s@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))
    tweet = re.sub('(@[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove punctuation
    my_punctuation = '!"$%&\'()*+,-./:;<=>?[\\]^_`{|}~•@â'
    tweet = re.sub('[' + my_punctuation + ']+', ' ', str(tweet))

    # Remove emoji
    emoji_pattern = re.compile("["
                               u"\U0001F600-\U0001F64F"  # emoticons
                               u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                               u"\U0001F680-\U0001F6FF"  # transport & map symbols
                               u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                               "]+", flags=re.UNICODE)
    tweet = emoji_pattern.sub(r'', tweet)  # no emoji
    # Remove numbers
    tweet = re.sub('([0-9]+)', '', str(tweet))

    # Remove hashtags
    tweet = re.sub('(#[A-Za-z]+[A-Za-z0-9-_]+)', '', str(tweet))

    # Remove extra simbols
    tweet = re.sub('@\w+', '', str(tweet))
    tweet = re.sub('\n', '', str(tweet))

    return tweet.strip()


class TweetListener(StreamingClient):

    def on_data(self, raw_data):
        # Log data to TW_DEBUG.log
        logging.info(raw_data)
        data = json.loads(raw_data.decode('utf-8'))
        cleaned_tweet = cleanTweet(data['data']['text'])
        df = spark.createDataFrame([[cleaned_tweet]], ['text'])
        df = df.withColumnRenamed('value', 'text')
        model = CrossValidatorModel.load('trained_model')
        predictionsDF = model.transform(df).select(['text', 'prediction'])
        predictionsDF.show()
        producer.send(config.TopicName,
                      value=bytes(['Positive' if row[0] == 2 else
                                   'Neutral' if row[0] == 1 else
                                   'Negative'
                                   for row in predictionsDF.select('prediction').collect()][-1],
                                  'utf-8'
                                  )
                      )
        return True

    def on_error(self, status_code):
        # Error if disconnect
        if status_code == 420:
            return False

    def start_streaming_tweets(self, search_term):
        rule = StreamRule(value=search_term)
        self.add_rules(rule)
        self.filter()


if __name__ == '__main__':
    # Creat logging instance
    logging.basicConfig(filename='TW_DEBUG.log', level=logging.DEBUG)

    # TWitter API usage
    twitter_stream = TweetListener(config.BearerToken)
    twitter_stream.start_streaming_tweets(config.SearchTerm)
