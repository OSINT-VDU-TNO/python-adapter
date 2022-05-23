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
            "produce": ["standard_cap"]
        }

        test_bed_adapter = TestBedAdapter(TestBedOptions(options))

        # This funcion will act as a handler. It only prints the message once it has been sent
        def message_sent_handler(message): return logging.info(
            "\n\n------\nmessage sent:\n------\n\n" + str(message))

        # Here we add the handler to the test bed adapter for on_sent
        test_bed_adapter.on_sent += message_sent_handler

        message1 = {"message": parse_json_file("cap.json")}
        messages1 = 1 * [message1]

        test_bed_adapter.initialize()

        test_bed_adapter.producer_managers["standard_cap"].send_messages(
            messages1)

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
