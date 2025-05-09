import time
import datetime
import logging  # Import logging
from typing import Optional  # Import Optional

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

from ..options.test_bed_options import TestBedOptions
from ..utils.key import generate_key


class ProducerManager:
    def __init__(self, options: TestBedOptions, kafka_topic: str):
        self.logger = logging.getLogger(__name__)  # Add logger
        self.options = options
        self.kafka_topic = kafka_topic

        # Each ProducerManager creates its own SchemaRegistryClient
        # This client instance will live as long as the ProducerManager instance
        try:
            schema_registry_conf = {"url": self.options.schema_registry}
            self.schema_registry_client: Optional[SchemaRegistryClient] = (
                SchemaRegistryClient(schema_registry_conf)
            )
            self.logger.info(
                f"ProducerManager initialized Schema Registry Client for {self.options.schema_registry}"
            )
        except Exception as e:
            self.logger.error(
                f"ProducerManager failed to initialize Schema Registry Client: {e}"
            )
            self.schema_registry_client = (
                None  # Ensure client is None if initialization fails
            )
            # Depending on requirements, you might want to raise the exception
            # raise

        # Only proceed if the Schema Registry client was successfully initialized
        if self.schema_registry_client is None:
            self.logger.error(
                "Schema Registry Client not available. ProducerManager cannot initialize Avro serializers."
            )
            self.producer: Optional[SerializingProducer] = (
                None  # Ensure producer is None
            )
            self.avro_message_serializer = None
            self.avro_key_serializer = None
            self.schema_str = None
            return  # Exit __init__ early

        try:
            # Use the stored schema_registry_client instance for serializers
            value_schema = self.schema_registry_client.get_latest_version(
                f"{kafka_topic}-value"
            ).schema.schema_str
            avro_message_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,  # Use self.client
                schema_str=value_schema,
            )

            key_schema = self.schema_registry_client.get_latest_version(
                f"{kafka_topic}-key"
            ).schema.schema_str
            avro_key_serializer = AvroSerializer(
                schema_registry_client=self.schema_registry_client,  # Use self.client
                schema_str=key_schema,
            )

            # Keep schema_str if needed elsewhere, otherwise this could be removed
            # self.schema = self.schema_registry_client.get_latest_version(kafka_topic + "-value") # Redundant unless self.schema is used
            self.schema_str: str = value_schema  # Store the schema string if needed

            producer_conf = {
                "bootstrap.servers": self.options.kafka_host,
                "key.serializer": avro_key_serializer,  # Use the created serializers
                "value.serializer": avro_message_serializer,  # Use the created serializers
                "partitioner": self.options.partitioner,
                "message.max.bytes": self.options.message_max_bytes,
                "compression.type": "gzip",
                "linger.ms": 5,  # Add a small linger.ms for basic batching even with flush outside loop
                # Consider adding retry logic and delivery timeout here
                # 'retries': 3,
                # 'delivery.timeout.ms': 60000,
            }
            # Store serializers and producer
            self.avro_message_serializer = avro_message_serializer
            self.avro_key_serializer = avro_key_serializer
            self.producer = SerializingProducer(producer_conf)
            self.logger.info(
                f"ProducerManager initialized for topic: {self.kafka_topic}"
            )

        except Exception as e:
            self.logger.error(
                f"ProducerManager failed to initialize producer for topic {self.kafka_topic}: {e}"
            )
            self.producer = None  # Ensure producer is None on failure
            # Clean up serializers if they were created before the error
            self.avro_message_serializer = None
            self.avro_key_serializer = None
            self.schema_str = None
            # The schema_registry_client instance still exists but might be unusable depending on the error
            # Depending on requirements, you might want to raise the exception
            # raise

    def send_messages(self, messages: list):
        if self.producer is None:
            self.logger.error("Producer is not initialized. Cannot send messages.")
            return

        for m in messages:
            # Calculate correct UTC timestamp in milliseconds
            date_ms = int(
                datetime.datetime.now(datetime.timezone.utc).timestamp() * 1000
            )

            k = generate_key(m, self.options)

            # Poll allows the producer's background thread to handle events (deliveries, errors)
            self.producer.poll(0.0)

            try:
                # Enqueue the message
                self.producer.produce(
                    topic=self.kafka_topic, key=k, value=m, timestamp=date_ms
                )
            except ValueError:
                self.logger.warning(
                    "Invalid input for serialization, discarding record..."
                )
                continue
            except BufferError:
                self.logger.warning("Producer queue full. Polling to clear...")
                # If the queue is full, poll with a timeout to give it time to send
                self.producer.poll(1.0)
                try:
                    # Try producing again after polling
                    self.producer.produce(
                        topic=self.kafka_topic, key=k, value=m, timestamp=date_ms
                    )
                except Exception as e:
                    self.logger.error(f"Failed to produce message after polling: {e}")
            except Exception as e:
                self.logger.error(f"Error producing message: {e}")

        # Flush all enqueued messages *after* the loop for batching.
        # This call will block until all messages are delivered or timeout.
        try:
            remaining_messages = self.producer.flush()
            if remaining_messages > 0:
                self.logger.warning(
                    f"Failed to flush {remaining_messages} message(s) to topic {self.kafka_topic}"
                )
            # else:
            #     self.logger.debug(f"Successfully flushed {len(messages)} messages for topic {self.kafka_topic}")
        except Exception as e:
            self.logger.error(
                f"Error during producer flush for topic {self.kafka_topic}: {e}"
            )

    def stop(self):
        """Ensures all messages in the queue are sent before stopping."""
        self.logger.info(
            f"Stopping producer for topic {self.kafka_topic}, flushing remaining messages..."
        )
        if self.producer:
            try:
                # Flush any outstanding messages
                remaining_messages = self.producer.flush()
                if remaining_messages > 0:
                    self.logger.warning(
                        f"{remaining_messages} message(s) could not be flushed before stopping for topic {self.kafka_topic}."
                    )
            except Exception as e:
                self.logger.error(
                    f"Error during producer stop/flush for topic {self.kafka_topic}: {e}"
                )
        self.logger.info(f"Producer for topic {self.kafka_topic} stopped.")
