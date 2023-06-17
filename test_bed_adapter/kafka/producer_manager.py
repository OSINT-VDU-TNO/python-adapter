import time
from datetime import datetime

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from ..options.test_bed_options import TestBedOptions
from ..utils.key import generate_key


class ProducerManager:
    def __init__(self, options: TestBedOptions, kafka_topic):
        self.options = options
        self.kafka_topic = kafka_topic

        schema_registry_conf = {'url': self.options.schema_registry}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        avro_message_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_registry_client.get_latest_version(str(kafka_topic + '-value')).schema.schema_str)
        avro_key_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client,
            schema_str=schema_registry_client.get_latest_version(str(kafka_topic + '-key')).schema.schema_str)
        self.schema = schema_registry_client.get_latest_version(kafka_topic + "-value")
        self.schema_str = self.schema.schema.schema_str
        producer_conf = {'bootstrap.servers': self.options.kafka_host,
                         'key.serializer': avro_key_serializer,
                         'value.serializer': avro_message_serializer,
                         'partitioner': self.options.partitioner}

        self.producer = SerializingProducer(producer_conf)

    def send_messages(self, messages: list):
        for m in messages:
            date = datetime.utcnow()
            date_ms = int(time.mktime(date.timetuple())) * 1000
            k = generate_key(m, self.options)
            self.producer.poll(0.0)
            try:
                self.producer.produce(topic=self.kafka_topic, key=k, value=m, timestamp=date_ms)
            except ValueError:
                print("Invalid input, discarding record...")
                continue

            self.producer.flush()

    def stop(self):
        self.producer.flush()
