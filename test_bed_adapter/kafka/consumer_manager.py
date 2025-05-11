import logging
from threading import Thread, Event, Lock
from time import sleep, time
import threading
from typing import Any, Dict, Literal, Tuple, Union

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..options.test_bed_options import TestBedOptions


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

        # Control flow events
        self._stop_event = Event()

        # Processing state
        self._processing_lock = Lock()
        self._processing_flag = False

        # Health monitoring state
        self._health_lock = Lock()
        self._health_status = {
            "status": "INITIALIZING",
            "message_processing_count": 0,
            "error_count": 0,
            "last_error": None,
            "last_error_time": None,
            "max_poll_interval_exceeded": False,
            "current_assigned_partitions": 0,
        }

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

        self._update_health_status("RUNNING")

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
            self._update_health_status("RUNNING")

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
        # All checks passed, consumer is healthy
        return 200, "OK", status_copy

    def run_auto_commit_mode(self):
        """Run in auto-commit mode - process messages in batches with auto-commit"""
        self.logger.info(f"Starting auto-commit consumer for {self.kafka_topic}")

        while not self._stop_event.is_set() and self.running:
            try:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)

                # Update partition count
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
                        # If error was not handled as a normal condition, update health status
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
                    # The failed message will be auto-committed
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
                    # Small delay to prevent tight error loops
                    sleep(1.0)

    def run_manual_commit_mode(self):
        """Run in manual-commit mode - process one message at a time with explicit commits"""
        self.logger.info(f"Starting manual-commit consumer for {self.kafka_topic}")

        while not self._stop_event.is_set() and self.running:
            try:
                self._update_health_status("OK", last_poll_time=time())
                # Check if we're still processing a message
                with self._processing_lock:
                    if self._processing_flag:
                        # Continue polling with a short timeout to maintain consumer heartbeat
                        # but don't fetch or process messages
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
                        # If error was not handled as a normal condition, update health status
                        self._update_health_status(
                            "WARNING",
                            last_error=f"Kafka error: {msg.error()}",
                            last_error_time=time(),
                            error_count=self._health_status["error_count"] + 1,
                        )
                    continue

                # Got a valid message - process it
                with self._processing_lock:
                    # Mark that we're processing a message
                    self._processing_flag = True
                    # Pause the consumer for all assigned partitions
                    self.pause()

                # Process the message in a separate thread
                processing_thread = threading.Thread(
                    target=self._process_message_in_thread, args=(msg,), daemon=True
                )
                processing_thread.start()

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
                    # Small delay to prevent tight error loops
                    sleep(1.0)

    def _process_message_in_thread(self, msg):
        """Process a message in a separate thread and handle resuming the consumer"""
        try:
            value = msg.value()
            topic = msg.topic()

            self.logger.info(
                f"Processing message from {topic}[{msg.partition()}] at offset {msg.offset()}"
            )

            # Call the user's handler
            self._handle_message_callback(value, topic)

            # Commit the message
            self.consumer.commit(msg)

            self._update_health_status(
                "OK",
                last_message_processed_time=time(),
                message_processing_count=self._health_status["message_processing_count"]
                + 1,
            )
            self.logger.info(
                f"Successfully processed and committed: {topic}[{msg.partition()}] at offset {msg.offset()}"
            )

        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
            self._update_health_status(
                "WARNING",
                last_error=str(e),
                last_error_time=time(),
                error_count=self._health_status["error_count"] + 1,
            )
            # In manual mode, we still commit the message even if processing failed
            # to avoid getting stuck on a bad message
            try:
                self.consumer.commit(msg)
                self.logger.warning(
                    f"Committed failed message: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                )
            except Exception as commit_error:
                self.logger.error(f"Error committing message: {commit_error}")
                self._update_health_status(
                    "ERROR",
                    last_error=f"Commit error: {commit_error}",
                    last_error_time=time(),
                    error_count=self._health_status["error_count"] + 1,
                )

        finally:
            # Resume the consumer and clear the processing flag
            with self._processing_lock:
                self._processing_flag = False
                # Resume the consumer for all assigned partitions
                self.resume()

    def _handle_kafka_error(self, msg) -> bool:
        """
        Handle Kafka errors from poll.
        Returns True if the error was handled as a normal condition, False otherwise.
        """
        error_code = msg.error().code()
        if error_code == KafkaError._PARTITION_EOF:
            # End of partition event - normal
            self.logger.debug(
                f"Reached end of partition: {msg.topic()} [{msg.partition()}]"
            )
            return True
        elif error_code == KafkaError._MAX_POLL_EXCEEDED:
            self.logger.error(
                f"MAX_POLL_EXCEEDED error: {msg.error()}. "
                "This indicates the consumer thread was blocked for too long. "
            )
            # Update health status with max poll exceeded flag
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
        offset_type="earliest",  # Start from earliest available message if no committed offset
    )

    kafka_topic = "your_avro_topic"

    # Choose the appropriate mode:
    # For lightweight processing: "auto_commit"
    # For resource-intensive processing: "manual_commit"
    processing_mode = "manual_commit"  # or "auto_commit"

    # Create and start the consumer
    consumer = ConsumerManager(
        options,
        kafka_topic,
        my_message_handler,
        processing_mode=processing_mode,
    )

    if consumer.running:  # Check if initialization was successful
        try:
            consumer.start()  # Start the consumer thread
            print(
                f"Consumer thread started in {processing_mode} mode. Press Ctrl+C to stop."
            )

            # Keep the main thread alive
            while consumer.is_alive():
                sleep(1)

        except KeyboardInterrupt:
            print("\nCtrl+C detected. Stopping consumer...")
        finally:
            consumer.stop()  # Signal the consumer thread to stop
            consumer.join(timeout=30)  # Wait for the consumer thread to finish
            print("Consumer thread stopped.")
    else:
        print("Consumer failed to initialize.")
