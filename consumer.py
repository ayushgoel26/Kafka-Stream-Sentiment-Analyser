from elasticsearch import Elasticsearch
from kafka import KafkaConsumer
import config
from datetime import datetime

es = Elasticsearch("https://127.0.0.1:9200", basic_auth=('elastic', 'gsPLmEOo4GeEx9s3ma59'), verify_certs=False)


def main():
    consumer = KafkaConsumer(config.TopicName, auto_offset_reset='earliest')

    for msg in consumer:
        print(datetime.utcfromtimestamp(msg.timestamp/1000).isoformat())
        es.index(
                    index="tweet_analysis_kafka_elk_gta",
                    # doc_type="test_doc",
                    body={
                        "sentiment": msg.value.decode("utf-8"),
                        "timestamp": datetime.utcfromtimestamp(msg.timestamp/1000).isoformat(),

                    }
                )


if __name__ == "__main__":
    main()
