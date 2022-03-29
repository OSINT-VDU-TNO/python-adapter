import sys
import json
import os
import logging
import time
logging.basicConfig(level=logging.INFO)
sys.path += [".."]
from test_bed_adapter.options.test_bed_options import TestBedOptions
from test_bed_adapter import TestBedAdapter


class ProducerExample:
    @staticmethod
    def main(schema_topic, body):
        options = {
            "auto_register_schemas": True,
            "kafka_host": '127.0.0.1:3501',
            "schema_registry": 'http://localhost:3502',
            "fetch_all_versions": False,
            "from_off_set": True,
            "client_id": 'RSS URL PRODUCER',
            "heartbeat_interval": 10,
            "produce": [schema_topic],
            "schema_folder": '../data/schemas'
        }

        test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        message = {"message": body}
        messages = 1 * [message]

        # This funcion will act as a handler. It only prints the message once it has been sent
        message_sent_handler = lambda message : logging.info("\n\n------\nmessage sent:\n------\n\n" + str(message))

        # Here we add the message to the test bed adapter
        test_bed_adapter.on_sent += message_sent_handler

        test_bed_adapter.initialize()
        test_bed_adapter.producer_managers[schema_topic].send_messages(messages)

        time.sleep(10)

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
    rss_urls_dict = parse_json_file("example_rss_urls.json")
    ProducerExample().main("system_rss_urls", rss_urls_dict)
