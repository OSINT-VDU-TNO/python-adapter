import os
import logging
import threading
from threading import Thread, Event, Lock
from time import sleep, time
from typing import Any, Dict, Literal, Tuple, Union, Optional
from queue import Queue, Empty

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..options.test_bed_options import TestBedOptions


class MessageProcessor(Thread):
    """Worker thread that processes messages from a queue."""

    def __init__(self, process_callback, consumer_manager):
        super().__init__()
        self.daemon = True
        self.logger = logging.getLogger(__name__)
        self.process_callback = process_callback
        self.consumer_manager = consumer_manager
        self.queue = Queue()
        self._stop_event = Event()
        self.name = "MessageProcessorThread"

    def run(self):
        """Main thread execution loop that processes messages from the queue."""
        self.logger.info(f"Message processor thread started")

        while not self._stop_event.is_set():
            try:
                # Get the next message from the queue with a timeout
                # This allows the thread to check the stop event periodically
                try:
                    msg = self.queue.get(timeout=1.0)
                except Empty:
                    continue

                # Process the message
                try:
                    self.logger.info(
                        f"Processing message from {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                    )

                    # Process the message using the callback
                    self.process_callback(msg.value(), msg.topic())

                    self.logger.info(
                        f"Successfully processed message from {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                    )

                    # Update consumer health metrics
                    self.consumer_manager._update_health_status(
                        "OK",
                        last_message_processed_time=time(),
                        message_processing_count=self.consumer_manager._health_status[
                            "message_processing_count"
                        ]
                        + 1,
                    )

                except Exception as e:
                    self.logger.error(f"Error processing message: {e}", exc_info=True)
                    self.consumer_manager._update_health_status(
                        "WARNING",
                        last_error=str(e),
                        last_error_time=time(),
                        error_count=self.consumer_manager._health_status["error_count"]
                        + 1,
                    )

                finally:
                    # In manual mode, commit the message even if processing failed
                    try:
                        self.consumer_manager.consumer.commit(msg, asynchronous=True)
                        self.logger.debug(
                            f"Committed message: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                        )
                    except Exception as commit_error:
                        self.logger.error(f"Error committing message: {commit_error}")
                        self.consumer_manager._update_health_status(
                            "ERROR",
                            last_error=f"Commit error: {commit_error}",
                            last_error_time=time(),
                            error_count=self.consumer_manager._health_status[
                                "error_count"
                            ]
                            + 1,
                        )

                    # Signal that we're done with this message
                    self.queue.task_done()

                    # Resume the consumer after processing is complete
                    with self.consumer_manager._processing_lock:
                        self.consumer_manager._processing_flag = False
                        self.consumer_manager.resume()

            except Exception as e:
                self.logger.error(
                    f"Unexpected error in message processor: {e}", exc_info=True
                )
                # Sleep briefly to prevent CPU spinning in case of repeated errors
                sleep(0.5)

    def process_message(self, msg):
        """Add a message to the processing queue."""
        self.queue.put(msg)

    def stop(self):
        """Signal the processor to stop."""
        self._stop_event.set()
        self.logger.info("Message processor stopping")


class ConsumerManager(Thread):
    def __init__(
        self,
        options: TestBedOptions,
        kafka_topic,
        handle_message,
        *,
        processing_mode="auto_commit",
    ):
        """
        Initialize the Kafka consumer.

        Args:
            options: Configuration options
            kafka_topic: Topic to consume from
            handle_message: Callback function for message processing
            processing_mode: Either "auto_commit" or "manual_commit"
                - auto_commit: For lightweight processing, processes messages in batch with auto commits
                - manual_commit: For resource-intensive tasks, processes one message at a time
        """
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.running = True
        self.daemon = True
        self.options = options
        self._handle_message_callback = handle_message
        self.kafka_topic = kafka_topic

        # Processing configuration
        self.processing_mode = processing_mode
        self.processing_timeout_ms = options.processing_timeout * 1000
        self.commit_on_timeout = True

        # Control flow events
        self._stop_event = Event()

        # Processing state
        self._processing_lock = Lock()
        self._processing_flag = False

        # Health monitoring state
        self._health_lock = Lock()
        self._health_status = {
            "status": "INITIALIZING",
            "last_message_processed_time": None,
            "last_poll_time": time(),
            "message_processing_count": 0,
            "error_count": 0,
            "timeout_count": 0,
            "last_error": None,
            "last_error_time": None,
            "max_poll_interval_exceeded": False,
            "current_assigned_partitions": 0,
        }

        # Create message processor thread for manual commit mode
        self.message_processor = None
        if processing_mode == "manual_commit":
            self.message_processor = MessageProcessor(
                self._handle_message_callback, self
            )
            self.message_processor.start()

        # --- Schema Registry and Deserializer Setup ---
        try:
            sr_conf = {"url": self.options.schema_registry}
            schema_registry_client = SchemaRegistryClient(sr_conf)
            self.avro_deserializer = AvroDeserializer(schema_registry_client)
            self._update_health_status("READY")
        except Exception as e:
            self.logger.error(f"Failed to initialize Schema Registry: {e}")
            self.running = False
            self._update_health_status(
                "ERROR", last_error=str(e), last_error_time=time()
            )

        # --- Configure Consumer Based on Mode ---
        consumer_conf = self._build_consumer_config()

        # Initialize the consumer
        self.consumer = None
        try:
            self.consumer = DeserializingConsumer(consumer_conf)
            self.consumer.subscribe([kafka_topic])
            self.logger.info(
                f"Kafka Consumer initialized for topic: {kafka_topic} in {processing_mode} mode"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka Consumer: {e}")
            self.running = False

    def _build_consumer_config(self):
        """Build the Kafka consumer configuration based on the processing mode"""
        consumer_conf = {
            "bootstrap.servers": self.options.kafka_host,
            "key.deserializer": self.avro_deserializer,
            "value.deserializer": self.avro_deserializer,
            "group.id": self.options.consumer_group,
            "message.max.bytes": self.options.message_max_bytes,
            "auto.offset.reset": self.options.offset_type,
            "session.timeout.ms": self.options.session_timeout_ms,
        }

        # Mode-specific configurations
        if self.processing_mode == "auto_commit":
            consumer_conf.update(
                {
                    "enable.auto.commit": True,
                    "auto.commit.interval.ms": 5000,  # Auto-commit every 5 seconds
                    "max.poll.interval.ms": 300000,  # 5 minutes max between polls
                }
            )
        else:  # manual_commit mode
            consumer_conf.update(
                {
                    "enable.auto.commit": False,
                    "max.poll.interval.ms": self.options.max_poll_interval_ms,
                }
            )

        return consumer_conf

    def run(self):
        """Main thread execution method"""
        if not self.running or self.consumer is None:
            self.logger.error("Consumer failed to initialize. Exiting run.")
            return

        self._update_health_status("OK")

        # Start processing based on mode
        if self.processing_mode == "auto_commit":
            self.run_auto_commit_mode()
        else:
            self.run_manual_commit_mode()

        # Close the consumer
        if self.consumer:
            self.consumer.close()
            self.logger.info(f"Consumer for {self.kafka_topic} closed.")

        self._update_health_status("STOPPED")

    def stop(self):
        """Signal the consumer to stop"""
        self.logger.info(f"Stopping consumer for {self.kafka_topic}")
        self._stop_event.set()
        self.running = False

        # Stop the message processor if it exists
        if self.message_processor:
            self.message_processor.stop()

        self._update_health_status("STOPPING")

    def pause(self):
        """Pause consuming messages"""
        assigned_partitions = self.consumer.assignment()
        if assigned_partitions:
            self.consumer.pause(assigned_partitions)
            self.logger.debug(f"Paused consumer for {assigned_partitions}")
            self._update_health_status("PAUSED")

    def resume(self):
        """Resume consuming messages"""
        assigned_partitions = self.consumer.assignment()
        if assigned_partitions:
            self.consumer.resume(assigned_partitions)
            self.logger.debug(f"Resumed consumer for {assigned_partitions}")
            self._update_health_status("OK")

    def _update_health_status(self, status, **kwargs):
        """Update the health status with new information"""
        with self._health_lock:
            self._health_status["status"] = status
            for key, value in kwargs.items():
                if key in self._health_status:
                    self._health_status[key] = value

    def get_health_status(
        self,
    ) -> Tuple[
        Union[Literal[200], Literal[503]],
        Union[Literal["OK"], Literal["ERROR"]],
        Dict[str, Any],
    ]:
        """
        Get the current health status of the consumer.

        Returns a tuple of (http_status_code=200 when OK or 503 otherwise, status_message=ERROR or OK, details_dict)
        """
        with self._health_lock:
            status_copy = self._health_status.copy()
        # Determine if the consumer is healthy based on various factors
        if status_copy["status"] == "ERROR" or not self.running:
            return 503, "ERROR", status_copy
        if status_copy["max_poll_interval_exceeded"]:
            return 503, "ERROR", status_copy
        if (
            status_copy["last_message_processed_time"] is not None
            and time() - status_copy["last_message_processed_time"]
            > self.processing_timeout_ms
        ):
            return 503, "ERROR", status_copy
        return 200, "OK", status_copy

    def run_auto_commit_mode(self):
        """Run in auto-commit mode - process messages in batches with auto-commit"""
        self.logger.info(f"Starting auto-commit consumer for {self.kafka_topic}")

        while not self._stop_event.is_set() and self.running:
            try:
                self._update_health_status("OK", last_poll_time=time())
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                # Update partition count
                assigned_partitions = self.consumer.assignment()
                self._update_health_status(
                    "OK",
                    last_message_processed_time=time(),
                    current_assigned_partitions=(
                        len(assigned_partitions) if assigned_partitions else 0
                    ),
                )

                if msg is None:
                    continue

                if msg.error():
                    error_handled = self._handle_kafka_error(msg)
                    if not error_handled:
                        self._update_health_status(
                            "WARNING",
                            last_error=f"Kafka error: {msg.error()}",
                            last_error_time=time(),
                            error_count=self._health_status["error_count"] + 1,
                        )
                    continue

                # Process the message directly in this thread
                try:
                    self.logger.info(
                        f"Processing message from {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                    )
                    self._handle_message_callback(msg.value(), msg.topic())
                    self._update_health_status(
                        "OK",
                        last_message_processed_time=time(),
                        message_processing_count=self._health_status[
                            "message_processing_count"
                        ]
                        + 1,
                    )
                    self.logger.info(
                        f"Successfully processed message: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                    )
                except Exception as e:
                    # In auto-commit mode, we log the error but continue processing
                    self.logger.error(f"Error processing message: {e}", exc_info=True)
                    self._update_health_status(
                        "WARNING",
                        last_error=str(e),
                        last_error_time=time(),
                        error_count=self._health_status["error_count"] + 1,
                    )

            except Exception as e:
                self.logger.error(
                    f"Unexpected error in consumer loop: {e}", exc_info=True
                )
                self._update_health_status(
                    "ERROR",
                    last_error=str(e),
                    last_error_time=time(),
                    error_count=self._health_status["error_count"] + 1,
                )
                if self.running:
                    sleep(1.0)

    def run_manual_commit_mode(self):
        """Run in manual-commit mode - process one message at a time using the persistent worker thread"""
        self.logger.info(f"Starting manual-commit consumer for {self.kafka_topic}")

        while not self._stop_event.is_set() and self.running:
            try:
                self._update_health_status("OK", last_poll_time=time())
                # Check if we're still processing a message
                with self._processing_lock:
                    if self._processing_flag:
                        self.consumer.poll(timeout=0.1)
                        sleep(0.1)
                        continue

                # Poll for a new message
                msg = self.consumer.poll(timeout=1.0)

                assigned_partitions = self.consumer.assignment()
                self._update_health_status(
                    "OK",
                    current_assigned_partitions=(
                        len(assigned_partitions) if assigned_partitions else 0
                    ),
                )

                if msg is None:
                    continue

                if msg.error():
                    error_handled = self._handle_kafka_error(msg)
                    if not error_handled:
                        self._update_health_status(
                            "WARNING",
                            last_error=f"Kafka error: {msg.error()}",
                            last_error_time=time(),
                            error_count=self._health_status["error_count"] + 1,
                        )
                    continue

                # Got a valid message - process it
                with self._processing_lock:
                    self._processing_flag = True
                    self.pause()  # Pause the consumer while processing

                # Instead of creating a new thread, send it to our persistent worker
                self.message_processor.process_message(msg)

            except Exception as e:
                self.logger.error(
                    f"Unexpected error in consumer loop: {e}", exc_info=True
                )
                self._update_health_status(
                    "ERROR",
                    last_error=str(e),
                    last_error_time=time(),
                    error_count=self._health_status["error_count"] + 1,
                )
                if self.running:
                    sleep(1.0)

    def _handle_kafka_error(self, msg) -> bool:
        """
        Handle Kafka errors from poll.
        Returns True if the error was handled as a normal condition, False otherwise.
        """
        error_code = msg.error().code()
        if error_code == KafkaError._PARTITION_EOF:
            self.logger.debug(
                f"Reached end of partition: {msg.topic()} [{msg.partition()}]"
            )
            return True
        elif error_code == KafkaError._MAX_POLL_EXCEEDED:
            self.logger.error(
                f"MAX_POLL_EXCEEDED error: {msg.error()}. "
                "This indicates the consumer thread was blocked for too long. "
            )
            self._update_health_status(
                "ERROR",
                max_poll_interval_exceeded=True,
                last_error=f"MAX_POLL_EXCEEDED: {msg.error()}",
                last_error_time=time(),
                error_count=self._health_status["error_count"] + 1,
            )
            return False
        elif error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
            self.logger.error(
                f"Kafka error: Topic or Partition unknown - {msg.error()}"
            )
            return False
        else:
            self.logger.error(f"Kafka error: {msg.error()}")
            return False


# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Your message handler function
    def my_message_handler(msg_value, topic_name):
        print(f"Handling message for {topic_name}: {msg_value}")
        # Simulate processing time
        import time

        print(f"Worker processing for 5 seconds...")
        time.sleep(5)
        print(f"Processing finished.")

    # Create options
    options = TestBedOptions(
        kafka_host="localhost:9092",
        schema_registry="localhost:8081",
        consumer_group="my_avro_consumer",
        max_poll_interval_ms=300000,  # 5 minutes
        session_timeout_ms=45000,  # 45 seconds
        offset_type="earliest",
    )

    kafka_topic = "your_avro_topic"
    processing_mode = "manual_commit"

    # Set environment variables for timeout
    os.environ["PROCESSING_TIMEOUT_SECONDS"] = "3"  # Set to 3 seconds for testing
    os.environ["COMMIT_ON_TIMEOUT"] = "true"  # Commit timed-out messages

    # Create and start the consumer
    consumer = ConsumerManager(
        options,
        kafka_topic,
        my_message_handler,
        processing_mode=processing_mode,
    )

    if consumer.running:
        try:
            consumer.start()
            print(
                f"Consumer thread started in {processing_mode} mode. Press Ctrl+C to stop."
            )
            while consumer.is_alive():
                sleep(1)
        except KeyboardInterrupt:
            print("\nCtrl+C detected. Stopping consumer...")
        finally:
            consumer.stop()
            consumer.join(timeout=30)
            print("Consumer thread stopped.")
    else:
        print("Consumer failed to initialize.")
