from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from datetime import datetime
from uuid import uuid4
import time

from naas_python_kafka.options.test_bed_options import TestBedOptions


class ProducerManager():
    def __init__(self, options: TestBedOptions, kafka_topic):
        self.options = options
        self.kafka_topic = kafka_topic

        schema_registry_conf = {'url': self.options.schema_registry}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)

        avro_message_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client, schema_str=schema_registry_client.get_latest_version(str(kafka_topic + '-value')).schema.schema_str)
        avro_key_serializer = AvroSerializer(
            schema_registry_client=schema_registry_client, schema_str=schema_registry_client.get_latest_version(str(kafka_topic + '-key')).schema.schema_str)

        producer_conf = {'bootstrap.servers': self.options.kafka_host,
                         'key.serializer': avro_key_serializer,
                         'value.serializer': avro_message_serializer,
                         'partitioner': self.options.partitioner}

        self.producer = SerializingProducer(producer_conf)

    def send_messages(self, messages: list):
        for m in messages:
            date = datetime.utcnow()
            date_ms = int(time.mktime(date.timetuple())) * 1000
            k = {"distributionID": str(uuid4()), "senderID": self.options.consumer_group,
                 "dateTimeSent": date_ms, "dateTimeExpires": 0,
                 "distributionStatus": "Test", "distributionKind": "Unknown"}
            # Serve on_delivery callbacks from previous calls to produce()
            self.producer.poll(0.0)
            try:
                self.producer.produce(
                    topic=self.kafka_topic, key=k, value=m, timestamp=date_ms)
            except ValueError:
                print("Invalid input, discarding record...")
                continue

            self.producer.flush()

    def stop(self):
        self.producer.flush()
