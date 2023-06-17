import copy
import datetime
import logging
import socket
import time
import urllib.request
from threading import Thread

from .producer_manager import ProducerManager
from ..options.test_bed_options import TestBedOptions


class HeartbeatManager(Thread):

    def __init__(self, options: TestBedOptions, kafka_topic):
        super().__init__()
        self.daemon = True
        self.logger = logging.getLogger(__name__)

        self.options = copy.deepcopy(options)
        self.options.string_key_type = 'group_id'
        self.running = True

        # Message parameters
        try:
            self.external_IP = str(urllib.request.urlopen("https://api.ipify.org").read().decode("utf-8"))
        except:
            self.external_IP = "unknown"
        self.host_name = str(socket.gethostname())
        self.host_IP = str(socket.gethostbyname(self.host_name))

        self.kafka_heartbeat_producer = ProducerManager(options=self.options, kafka_topic=kafka_topic)

    def run(self):
        self.logger.info("Heartbeat Started")
        while self.running:
            self.send_heartbeat_message()
            time.sleep(self.options.heartbeat_interval)

    def send_heartbeat_message(self):
        date = datetime.datetime.utcnow()
        date_ms = int(time.mktime(date.timetuple())) * 1000

        message_json = {
            "id": self.options.consumer_group,
            "alive": date_ms,
            "origin": "{hostname: %s, localIP: %s, externalIP: %s}" % (
                self.host_name, self.host_IP, self.external_IP)
        }
        self.kafka_heartbeat_producer.send_messages(messages=[message_json])

    def stop(self):
        self.kafka_heartbeat_producer.stop()
