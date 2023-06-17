import logging
import sys
import time

from test_bed_adapter import (TestBedAdapter, TestBedOptions)
from test_bed_adapter.kafka.consumer_manager import ConsumerManager

logging.basicConfig(level=logging.INFO)
sys.path += [".."]


class ConsumerExample:
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

        # This funcion will act as a handler. It only prints the incoming messages
        def handle_art_message(message, topic):
            return logging.info(
                "--Incoming article_raw_xx message:" + str(message) + "\n")

        def handle_conf_message(message, topic):
            return logging.info(
                "--Incoming config message:" + str(message) + "\n")

        # We initialize the process (catching schemas and so on) and we listen the messages from the topic system_rss_feeds
        test_bed_adapter.initialize()

        # Create a new thread that listens to system_rss_feeds topic on the background
        ConsumerManager(options=TESTBED_OPTIONS, kafka_topic='article_raw_xx',handle_message=handle_art_message).start()
        ConsumerManager(options=TESTBED_OPTIONS, kafka_topic='config', handle_message=handle_conf_message).start()
        try:
            # wait for some time
            time.sleep(60)
        finally:
            # Stop test bed
            test_bed_adapter.stop()


if __name__ == '__main__':
    ConsumerExample().main()
