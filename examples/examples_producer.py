import json
from naas_python_kafka import (TestBedAdapter, TestBedOptions)
from naas_python_kafka.kafka.producer_manager import ProducerManager
import sys
import logging
import os
import time
logging.basicConfig(level=logging.INFO)
sys.path += [".."]


class ProducerExample:
    @staticmethod
    def main():
        tb_options = {
            "consumer_group": 'EXAMPLES CONSUMER',
            "kafka_host": '127.0.0.1:3501',
            "schema_registry": 'http://localhost:3502',
            "message_max_bytes": 1000000,
            "partitioner": 'random',
            "offset_type": 'earliest'
        }

        TESTBED_OPTIONS = TestBedOptions(tb_options)

        test_bed_adapter = TestBedAdapter(TESTBED_OPTIONS)

        test_bed_adapter.initialize()

        article_raw_producer = ProducerManager(options=TESTBED_OPTIONS, kafka_topic='article_raw_xx')
        config_producer = ProducerManager(options=TESTBED_OPTIONS, kafka_topic='config')
        feed_producer = ProducerManager(options=TESTBED_OPTIONS, kafka_topic='feed_item_xx')
        metadata_producer = ProducerManager(options=TESTBED_OPTIONS, kafka_topic='metadata_item')
        source_producer = ProducerManager(options=TESTBED_OPTIONS, kafka_topic='source_item')

        article_raw_producer.send_messages(
            [parse_json_file("example_article.json")])
        config_producer.send_messages([parse_json_file("example_config.json")])
        feed_producer.send_messages([parse_json_file("example_feed.json")])
        metadata_producer.send_messages(
            [parse_json_file("example_metadata.json")])
        source_producer.send_messages([parse_json_file("example_source.json")])

        try:
            # wait for some time
            time.sleep(60)
        finally:
            # Stop test bed
            test_bed_adapter.stop()


def parse_json_file(file_name):
    message_path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                "sample_messages",
                                file_name)
    example_message_file = open(message_path, encoding="utf-8")
    body = json.loads(example_message_file.read())
    example_message_file.close()
    return body


if __name__ == '__main__':
    ProducerExample().main()
