from ..options.test_bed_options import TestBedOptions
from .producer_manager import ProducerManager
from ..utils.helpers import Helpers

import copy
import datetime
import urllib.request
import socket
import time


class HeartbeatManager:
    def __init__(self, options: TestBedOptions, kafka_topic):
        self.options = copy.deepcopy(options)
        self.options.string_key_type = 'group_id'

        self.helper = Helpers()
        self.interval_thread = {}

        self.kafka_heartbeat_producer = ProducerManager(
            options=self.options, kafka_topic=kafka_topic)

    def start_heartbeat_async(self):
        print('Heartbeat Started')
        self.interval_thread = self.helper.set_interval(
            self.send_heartbeat_message, self.options.heartbeat_interval)

    def send_heartbeat_message(self):
        date = datetime.datetime.utcnow()
        date_ms = int(time.mktime(date.timetuple())) * 1000

        # Get data for origin stringified json
        hostName = str(socket.gethostname())
        hostIP = str(socket.gethostbyname(hostName))
        try:
            externalIP = str(urllib.request.urlopen(
                "http://ipv4bot.whatismyipaddress.com").read().decode("utf-8"))
        except urllib.error.URLError as e:
            externalIP = "unknown"

        message_json = {"id": self.options.consumer_group, "alive": date_ms,
                        "origin": "{hostname: %s, localIP: %s, externalIP: %s}" % (hostName, hostIP, externalIP)}

        messages = [message_json]
        self.kafka_heartbeat_producer.send_messages(messages=messages)

    def stop(self):
        self.kafka_heartbeat_producer.stop()
