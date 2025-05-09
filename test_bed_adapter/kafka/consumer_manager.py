import logging
from threading import Thread, Event, Lock
from time import sleep
import concurrent.futures

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
        self._pause_event = Event()

        # Processing state
        self._processing_lock = Lock()
        self._currently_processing = False

        # --- Schema Registry and Deserializer Setup ---
        try:
            sr_conf = {"url": self.options.schema_registry}
            schema_registry_client = SchemaRegistryClient(sr_conf)
            self.avro_deserializer = AvroDeserializer(schema_registry_client)
        except Exception as e:
            self.logger.error(f"Failed to initialize Schema Registry: {e}")
            self.running = False

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

        # For manual mode, we'll use a thread pool with a single worker
        if self.processing_mode == "manual_commit":
            self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            self._current_task = None
        else:
            self.executor = None

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
                    # We'll handle limiting to one message per poll in our code
                    # since max.poll.records is not supported in confluent_kafka
                }
            )

        return consumer_conf

    def run(self):
        """Main thread execution method"""
        if not self.running or self.consumer is None:
            self.logger.error("Consumer failed to initialize. Exiting run.")
            if self.processing_mode == "manual_commit" and self.executor:
                self.executor.shutdown(wait=False)
            return

        # Start processing based on mode
        if self.processing_mode == "auto_commit":
            self.run_auto_commit_mode()
        else:
            self.run_manual_commit_mode()

        # Cleanup
        if self.processing_mode == "manual_commit" and self.executor:
            # Wait for any in-progress task
            if self._current_task:
                self.logger.info("Waiting for current task to complete...")
                concurrent.futures.wait([self._current_task], timeout=30.0)

            # Shutdown the executor
            self.executor.shutdown(wait=True)

        # Close the consumer
        if self.consumer:
            self.consumer.close()
            self.logger.info(f"Consumer for {self.kafka_topic} closed.")

    def stop(self):
        """Signal the consumer to stop"""
        self.logger.info(f"Stopping consumer for {self.kafka_topic}")
        self._stop_event.set()
        self.running = False

    def pause(self):
        """Pause consuming messages"""
        self._pause_event.set()
        self.logger.info(f"Paused consuming from {self.kafka_topic}")

    def resume(self):
        """Resume consuming messages"""
        self._pause_event.clear()
        self.logger.info(f"Resumed consuming from {self.kafka_topic}")

    def run_auto_commit_mode(self):
        """Run in auto-commit mode - process messages in batches with auto-commit"""
        self.logger.info(f"Starting auto-commit consumer for {self.kafka_topic}")

        while not self._stop_event.is_set() and self.running:
            if self._pause_event.is_set():
                sleep(0.5)
                continue

            try:
                # Poll for messages - in confluent_kafka this returns a single Message object,
                # not a batch of messages like in the Java client
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    self._handle_kafka_error(msg)
                    continue

                # Process the message directly in this thread
                try:
                    self.logger.info(
                        f"Processing message from {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                    )
                    self._handle_message_callback(msg.value(), msg.topic())
                    self.logger.info(
                        f"Successfully processed message: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                    )
                except Exception as e:
                    # In auto-commit mode, we log the error but continue processing
                    # The failed message will be auto-committed
                    self.logger.error(f"Error processing message: {e}", exc_info=True)

            except Exception as e:
                self.logger.error(
                    f"Unexpected error in consumer loop: {e}", exc_info=True
                )
                if self.running:
                    # Small delay to prevent tight error loops
                    sleep(1.0)

    def run_manual_commit_mode(self):
        """Run in manual-commit mode - process one message at a time with explicit commits"""
        self.logger.info(f"Starting manual-commit consumer for {self.kafka_topic}")

        while not self._stop_event.is_set() and self.running:
            # Check if we should pause
            if self._pause_event.is_set():
                sleep(0.5)
                continue

            # Check if we're still processing the last message
            if self._current_task and not self._current_task.done():
                # The previous message is still being processed
                # Poll with timeout=0 to maintain consumer group liveness
                # without fetching new messages
                self.consumer.poll(0)
                sleep(0.1)
                continue

            try:
                # In confluent_kafka, we don't have max.poll.records, so we handle one message at a time
                # manually from whatever batch was returned
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue  # No message received, poll again

                if msg.error():
                    self._handle_kafka_error(msg)
                    continue

                # Got a valid message - process it one at a time
                self._process_message_safely(msg)

                # Important: After processing one message, we immediately exit the polling loop
                # This simulates processing one message at a time even if more were fetched

            except Exception as e:
                self.logger.error(
                    f"Unexpected error in consumer loop: {e}", exc_info=True
                )
                if self.running:
                    # Small delay to prevent tight error loops
                    sleep(1.0)

    def _process_message_safely(self, msg):
        """Process a single message with proper error handling in manual mode"""
        # Pause the consumer while processing
        self.pause()

        # Mark that we're starting to process a message
        with self._processing_lock:
            self._currently_processing = True

        # Process in the worker thread
        self._current_task = self.executor.submit(self._process_single_message, msg)

        # Add a callback for when processing completes
        self._current_task.add_done_callback(self._on_message_processed)

    def _process_single_message(self, msg):
        """Process a single message in manual mode"""
        value = msg.value()
        topic = msg.topic()

        try:
            # Call the user's handler
            self.logger.info(f"Processing message from {topic}")
            self._handle_message_callback(value, topic)

            # Return success
            return {"status": "success", "msg": msg}

        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
            # Return failure with the message and error
            return {"status": "error", "msg": msg, "error": str(e)}

    def _on_message_processed(self, future):
        """Callback when message processing completes in manual mode"""
        try:
            # Get the result
            result = future.result()
            msg = result["msg"]

            self.consumer.commit(msg)
            if result["status"] == "success":
                # Message processed successfully, commit the offset
                self.logger.info(
                    f"Successfully processed and committed: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                )
            else:
                # Processing failed
                error = result.get("error", "Unknown error")
                self.logger.warning(
                    f"Failed message committed ({error}): {msg.topic()}[{msg.partition()}] at offset {msg.offset()}"
                )

        except Exception as e:
            self.logger.error(f"Error in message completion handling: {e}")

        finally:
            # Mark that we're done processing
            with self._processing_lock:
                self._currently_processing = False
            # Resume the consumer
            self.resume()

    def _handle_kafka_error(self, msg):
        """Handle Kafka errors from poll"""
        error_code = msg.error().code()
        if error_code == KafkaError._PARTITION_EOF:
            # End of partition event - normal
            self.logger.debug(
                f"Reached end of partition: {msg.topic()} [{msg.partition()}]"
            )
        elif error_code == KafkaError._MAX_POLL_EXCEEDED:
            self.logger.error(
                f"MAX_POLL_EXCEEDED error: {msg.error()}. "
                "This indicates the consumer thread was blocked for too long. "
            )
        elif error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
            self.logger.error(
                f"Kafka error: Topic or Partition unknown - {msg.error()}"
            )
        else:
            self.logger.error(f"Kafka error: {msg.error()}")


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
