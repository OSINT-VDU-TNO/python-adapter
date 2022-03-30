import time
import sys
import logging
import threading
logging.basicConfig(level=logging.INFO)
sys.path += [".."]
from test_bed_adapter.options.test_bed_options import TestBedOptions
from test_bed_adapter import TestBedAdapter

class ConsumerExample:
    @staticmethod
    def main():
        options = {
          "auto_register_schemas": True,
          "kafka_host": '127.0.0.1:3501',
          "schema_registry": 'http://localhost:3502',
          "fetch_all_versions": False,
          "from_off_set": True,
          "client_id": 'RSS READER',
          "consume": ['system_rss_feeds'],
          "schema_folder": '../data/schemas'
          }

        test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        # This funcion will act as a handler. It only prints the incoming messages
        handle_sys_rss_feeds_message = lambda message: logging.info("--Incoming system_rss_feeds message:" + str(message) + "\n")

        # We initialize the process (catching schemas and so on) and we listen the messages from the topic system_rss_feeds
        test_bed_adapter.initialize()

        # Add a handler to the test bed adapter only for input messages on system_rss_feeds topic
        test_bed_adapter.consumer_managers["system_rss_feeds"].on_message += handle_sys_rss_feeds_message

        # Create a new thread that listens to system_rss_feeds topic on the background
        system_rss_feeds_listener_thread_ = threading.Thread(target=test_bed_adapter.consumer_managers["system_rss_feeds"].listen_messages)
        system_rss_feeds_listener_thread_.start()

        # wait for some time
        time.sleep(60)

        # Stop test bed
        test_bed_adapter.stop()

        # Clean after ourselves
        system_rss_feeds_listener_thread_.join()


if __name__ == '__main__':
    ConsumerExample().main()
