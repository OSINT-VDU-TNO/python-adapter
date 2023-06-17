import logging
from threading import Thread
from time import time, sleep

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..options.test_bed_options import TestBedOptions


class ConsumerManager(Thread):
    def __init__(self, options: TestBedOptions, kafka_topic, handle_message):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.daemon = True
        self.options = options
        self.handle_message = handle_message
        self.running = True

        self.latest_message = None

        sr_conf = {'url': self.options.schema_registry}
        schema_registry_client = SchemaRegistryClient(sr_conf)

        self.avro_deserializer = AvroDeserializer(schema_registry_client)
        self.schema = schema_registry_client.get_latest_version(kafka_topic + "-value")
        self.schema_str = self.schema.schema.schema_str
        self.kafka_topic = kafka_topic

        consumer_conf = {
            'bootstrap.servers': self.options.kafka_host,
            'key.deserializer': self.avro_deserializer,
            'value.deserializer': self.avro_deserializer,
            'group.id': self.options.consumer_group,
            'message.max.bytes': self.options.message_max_bytes,
            'auto.offset.reset': self.options.offset_type
        }

        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([kafka_topic])

    def run(self):
        self.reset_partition_offsets()
        self.ingore_messages()
        self.use_latest_message()
        self.listen()

    def stop(self):
        self.running = False

    def reset_partition_offsets(self):
        """Reset partition offsets to beginning"""
        if self.options.offset_type != 'earliest':
            return
        try:
            # Need to poll to get assigned partitions
            self.latest_message = self.consumer.poll(1)
            # Wait for partitions to be assigned
            partitions = []
            while not partitions and self.latest_message is None:
                sleep(1)
                self.latest_message = self.consumer.poll(10)
                partitions = self.consumer.assignment()
            #  Reset partitions to beginning
            for partition in partitions:
                partition.offset = 0
                # Seek to beginning
                self.consumer.seek(partition)
                self.consumer.commit()
        except Exception as e:
            self.logger.error(f"Error resetting partition offsets: {e}")
            return

    def ingore_messages(self):
        """Ignore messages for a period of time"""
        _start_time = time()
        # Ignore messages for a period of time
        if not self.options.ignore_timeout:
            return
        while True:
            msg = self.consumer.poll(1)
            if msg:
                self.latest_message = msg
            elapsed_time = time() - _start_time
            if elapsed_time > self.options.ignore_timeout:
                break

    def use_latest_message(self):
        """Use the latest message on the topic"""
        if self.latest_message and self.options.use_latest:
            self.handle_message(self.latest_message.value(), self.latest_message.topic())

    def listen(self):
        """Listen for messages on the topic and handle them with the provided callback"""
        # Continue to listen for messages
        while self.running:
            msg = self.consumer.poll(1)
            if msg is None:
                continue
            self.handle_message(msg.value(), msg.topic())
        # Close down consumer to commit final offsets.
        self.consumer.close()
