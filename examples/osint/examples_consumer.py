from test_bed_adapter import TestBedAdapter
from test_bed_adapter.options.test_bed_options import TestBedOptions
import time
import sys
import logging
import threading
logging.basicConfig(level=logging.INFO)
sys.path += [".."]


class ConsumerExample:
    @staticmethod
    def main():
        options = {
            "kafka_host": '127.0.0.1:3501',
            "schema_registry": 'http://localhost:3502',
            "fetch_all_versions": False,
            "from_off_set": True,
            "client_id": 'EXAMPLES CONSUMER',
            "consume": ["article_raw_xx", "config", "feed_item_xx", "metadata_item", "source_item"]
        }

        test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        # This funcion will act as a handler. It only prints the incoming messages
        def handle_art_message(message): return logging.info(
            "--Incoming article_raw_xx message:" + str(message) + "\n")

        def handle_conf_message(message): return logging.info(
            "--Incoming config message:" + str(message) + "\n")

        def handle_feed_message(message): return logging.info(
            "--Incoming feed_item_xx message:" + str(message) + "\n")

        def handle_metadata_message(message): return logging.info(
            "--Incoming metadata_item message:" + str(message) + "\n")

        def handle_source_message(message): return logging.info(
            "--Incoming source_item message:" + str(message) + "\n")

        # We initialize the process (catching schemas and so on) and we listen the messages from the topic system_rss_feeds
        test_bed_adapter.initialize()

        # Add a handler to the test bed adapter only for input messages on system_rss_feeds topic
        test_bed_adapter.consumer_managers["article_raw_xx"].on_message += handle_art_message
        test_bed_adapter.consumer_managers["config"].on_message += handle_conf_message
        test_bed_adapter.consumer_managers["feed_item_xx"].on_message += handle_feed_message
        test_bed_adapter.consumer_managers["metadata_item"].on_message += handle_metadata_message
        test_bed_adapter.consumer_managers["source_item"].on_message += handle_source_message

        # Create a new thread that listens to system_rss_feeds topic on the background
        system_art_listener_thread_ = threading.Thread(
            target=test_bed_adapter.consumer_managers["article_raw_xx"].listen_messages)
        system_art_listener_thread_.start()
        system_conf_listener_thread_ = threading.Thread(
            target=test_bed_adapter.consumer_managers["config"].listen_messages)
        system_conf_listener_thread_.start()
        system_feed_listener_thread_ = threading.Thread(
            target=test_bed_adapter.consumer_managers["feed_item_xx"].listen_messages)
        system_feed_listener_thread_.start()
        system_metadata_listener_thread_ = threading.Thread(
            target=test_bed_adapter.consumer_managers["metadata_item"].listen_messages)
        system_metadata_listener_thread_.start()
        system_source_listener_thread_ = threading.Thread(
            target=test_bed_adapter.consumer_managers["source_item"].listen_messages)
        system_source_listener_thread_.start()

        try:
            # wait for some time
            time.sleep(60)
        finally:
            # Stop test bed
            test_bed_adapter.stop()

            # Clean after ourselves
            system_art_listener_thread_.join()
            system_conf_listener_thread_.join()
            system_feed_listener_thread_.join()
            system_metadata_listener_thread_.join()
            system_source_listener_thread_.join()


if __name__ == '__main__':
    ConsumerExample().main()
