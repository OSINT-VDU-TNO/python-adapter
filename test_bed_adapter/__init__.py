import logging
from typing import Optional

from confluent_kafka.schema_registry import SchemaRegistryClient

from .kafka.heartbeat_manager import HeartbeatManager
from .options.test_bed_options import TestBedOptions


class TestBedAdapter:
    def __init__(self, test_bed_options: TestBedOptions):
        self.logger = logging.getLogger(__name__)
        self.test_bed_options = test_bed_options
        self.default_producer_topics = ["system_heartbeat"]
        self.heartbeat_manager: HeartbeatManager = Optional[HeartbeatManager]

    def initialize(self):
        self.logger.info("Initializing test bed")
        self.init_and_start_heartbeat()

    def init_and_start_heartbeat(self):
        schema_registry_conf = {'url': self.test_bed_options.schema_registry}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        if "system_heartbeat-value" in schema_registry_client.get_subjects():
            self.heartbeat_manager = HeartbeatManager(
                options=self.test_bed_options, kafka_topic='system_heartbeat')
            self.heartbeat_manager.start()
        else:
            logging.error(
                "Heartbeat could not be initialized, No schema found for topic system_heartbeat")

    def stop(self):
        # Stop the heartbeat thread
        if self.heartbeat_manager is not None:
            self.heartbeat_manager.stop()
