import sys
import logging
import threading
import time
logging.basicConfig(level=logging.INFO)
sys.path += [".."]
from test_bed_adapter.options.test_bed_options import TestBedOptions
from test_bed_adapter import TestBedAdapter
import feedparser
import json

class ProducerExample:
    @staticmethod
    def main(schema_topic):
        options = {
            "auto_register_schemas": True,
            "kafka_host": '127.0.0.1:3501',
            "schema_registry": 'http://localhost:3502',
            "fetch_all_versions": False,
            "from_off_set": True,
            "client_id": 'RSS FEED PRODUCER',
            "heartbeat_interval": 10,
            "produce": [schema_topic],
            "consume": ['system_rss_urls'],
            "schema_folder": '../data/schemas'
        }

        test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        # This funcion will act as a handler. It only prints the message once it has been sent
        message_sent_handler = lambda message : logging.info("\n\n------\nmessage sent:\n------\n\n" + str(message))

        # Func when we receive system_rss_urls message
        handle_system_rss_urls_message = lambda message: handle_sys_rss_msg(message, test_bed_adapter, schema_topic)

        # Here we add the handler to the test bed adapter for on_sent
        test_bed_adapter.on_sent += message_sent_handler

        test_bed_adapter.initialize()

        # Add a handler to the test bed adapter only for input messages on system_rss_urls topic
        test_bed_adapter.consumer_managers["system_rss_urls"].on_message += handle_system_rss_urls_message

        # Create a new thread that listens to system_rss_urls topic on the background
        system_rss_urls_listener_thread_ = threading.Thread(target=test_bed_adapter.consumer_managers["system_rss_urls"].listen_messages)
        system_rss_urls_listener_thread_.start()

        # wait for some time
        time.sleep(60)

        # Stop test bed
        test_bed_adapter.stop()

        # Clean after ourselves
        system_rss_urls_listener_thread_.join()

def handle_sys_rss_msg(message, test_bed_adapter, schema_topic):
    messages = []
    for msg in message['decoded_value']:
        for url in msg['urls']:
            NewsFeed = feedparser.parse(url)
            entry = NewsFeed.entries[1]
            messages.append({'url': url, 'entry': json.dumps(entry)})

    test_bed_adapter.producer_managers[schema_topic].send_messages(messages)

if __name__ == '__main__':
    ProducerExample().main("system_rss_feeds")
