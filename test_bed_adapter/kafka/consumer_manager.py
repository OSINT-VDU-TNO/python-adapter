from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from naas_python_kafka.options.test_bed_options import TestBedOptions


class ConsumerManager():
    def __init__(self, options: TestBedOptions, kafka_topic, handle_message):
        self.options = options
        self.handle_message = handle_message

        sr_conf = {'url': self.options.schema_registry}
        schema_registry_client = SchemaRegistryClient(sr_conf)
        avro_deserializer = AvroDeserializer(schema_registry_client)

        consumer_conf = {'bootstrap.servers': self.options.kafka_host,
                         'key.deserializer': avro_deserializer,
                         'value.deserializer': avro_deserializer,
                         'group.id': self.options.consumer_group,
                         'message.max.bytes': self.options.message_max_bytes,
                         'auto.offset.reset': self.options.offset_type}

        self.consumer = DeserializingConsumer(consumer_conf)
        self.consumer.subscribe([kafka_topic])

    def listen(self):
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue

                self.handle_message(msg.value())
            except KeyboardInterrupt:
                break
        self.consumer.close()

    def stop(self):
        self.consumer.close()
