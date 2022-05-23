import json
from test_bed_adapter import TestBedAdapter
from test_bed_adapter.options.test_bed_options import TestBedOptions
import sys
import logging
import os
import time
logging.basicConfig(level=logging.INFO)
sys.path += [".."]


class ProducerExample:
    @staticmethod
    def main():
        options = {
            "kafka_host": '127.0.0.1:3501',
            "schema_registry": 'http://localhost:3502',
            "fetch_all_versions": False,
            "from_off_set": True,
            "client_id": 'EXAMPLES PRODUCERS',
            "produce": ["article_raw_xx", "config", "feed_item_xx", "metadata_item", "source_item"]
        }

        test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        # This funcion will act as a handler. It only prints the message once it has been sent
        def message_sent_handler(message): return logging.info(
            "\n\n------\nmessage sent:\n------\n\n" + str(message))

        # Here we add the handler to the test bed adapter for on_sent
        test_bed_adapter.on_sent += message_sent_handler

        message1 = {"message": parse_json_file("example_article.json")}
        messages1 = 1 * [message1]
        message2 = {"message": parse_json_file("example_config.json")}
        messages2 = 1 * [message2]
        message3 = {"message": parse_json_file("example_feed.json")}
        messages3 = 1 * [message3]
        message4 = {"message": parse_json_file("example_metadata.json")}
        messages4 = 1 * [message4]
        message5 = {"message": parse_json_file("example_source.json")}
        messages5 = 1 * [message5]

        test_bed_adapter.initialize()

        test_bed_adapter.producer_managers["article_raw_xx"].send_messages(
            messages1)
        test_bed_adapter.producer_managers["config"].send_messages(messages2)
        test_bed_adapter.producer_managers["feed_item_xx"].send_messages(
            messages3)
        test_bed_adapter.producer_managers["metadata_item"].send_messages(
            messages4)
        test_bed_adapter.producer_managers["source_item"].send_messages(
            messages5)

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
