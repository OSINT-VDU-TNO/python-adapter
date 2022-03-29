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
          "consume": ["system_rss_feeds"],
          "schema_folder": '../data/schemas'
          }

        test_bed_options = TestBedOptions(options)
        test_bed_adapter = TestBedAdapter(test_bed_options)

        # This funcion will act as a handler. It only prints the incoming messages
        handle_sys_rss_feeds_message = lambda message: logging.info("--Incoming system_rss_feeds message:" + str(message) + "\n")

        # We initialize the process (catching schemas and so on) and we listen the messages from the topic standard_cap
        test_bed_adapter.initialize()

        # Add a handler to the test bed adapter only for input messages on standard_cap topic
        test_bed_adapter.consumer_managers["system_rss_feeds"].on_message += handle_sys_rss_feeds_message

        # Create a new thread that listens to standard_cap topic on the background
        standard_cap_listener_thread_ = threading.Thread(target=test_bed_adapter.consumer_managers["system_timing"].listen_messages)
        standard_cap_listener_thread_.start()

        # wait for some time
        time.sleep(60)

        # Stop test bed
        test_bed_adapter.stop()

        # Clean after ourselves
        standard_cap_listener_thread_.join()


if __name__ == '__main__':
    ConsumerExample().main()
