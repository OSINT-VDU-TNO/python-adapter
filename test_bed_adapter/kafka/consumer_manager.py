from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..options.test_bed_options import TestBedOptions


class ConsumerManager():
    def __init__(self, options: TestBedOptions, kafka_topic, handle_message, run):
        self.options = options
        self.handle_message = handle_message
        self.run = run

        sr_conf = {'url': self.options.schema_registry}
        schema_registry_client = SchemaRegistryClient(sr_conf)
        self.avro_deserializer = AvroDeserializer(schema_registry_client)
        self.schema = schema_registry_client.get_latest_version(kafka_topic + "-value")
        self.schema_str = self.schema.schema.schema_str
        self.kafka_topic = kafka_topic

        consumer_conf = {'bootstrap.servers': self.options.kafka_host,
                         'key.deserializer': self.avro_deserializer,
                         'value.deserializer': self.avro_deserializer,
                         'group.id': self.options.consumer_group,
                         'message.max.bytes': self.options.message_max_bytes,
                         'auto.offset.reset': self.options.offset_type}

        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([kafka_topic])

    def listen(self):
        while self.run():
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue

            self.handle_message(msg.value())
        self.consumer.close()

    def stop(self):
        self.consumer.close()
