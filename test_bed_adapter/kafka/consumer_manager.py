import logging
from threading import Thread
from time import sleep
import concurrent.futures # Import ThreadPoolExecutor

from confluent_kafka import DeserializingConsumer, TopicPartition, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..options.test_bed_options import TestBedOptions

class ConsumerManager(Thread):
    def __init__(self, options: TestBedOptions, kafka_topic, handle_message):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.running = True
        self.daemon = True # Allow program to exit even if this thread is running
        self.options = options
        self._handle_message_callback = handle_message # Store the user's handler

        # --- Schema Registry and Deserializer Setup ---
        try:
            sr_conf = {'url': self.options.schema_registry}
            schema_registry_client = SchemaRegistryClient(sr_conf)
            self.avro_deserializer = AvroDeserializer(schema_registry_client)

            # You might want to fetch schema proactively, but be mindful of errors
            # If schema lookup fails here, the consumer will likely fail later anyway
            # self.schema = schema_registry_client.get_latest_version(kafka_topic + "-value")
            # self.schema_str = self.schema.schema.schema_str # Storing schema string might be useful

        except Exception as e:
             self.logger.error(f"Failed to initialize Schema Registry or fetch schema: {e}")
             # Depending on requirements, you might raise the exception
             self.running = False # Prevent thread from starting listen loop

        self.kafka_topic = kafka_topic

        # --- Kafka Consumer Configuration ---
        consumer_conf = {
            'bootstrap.servers': self.options.kafka_host,
            'key.deserializer': self.avro_deserializer,
            'value.deserializer': self.avro_deserializer,
            'group.id': self.options.consumer_group,
            'message.max.bytes': self.options.message_max_bytes,
            'auto.offset.reset': self.options.offset_type,
            'max.poll.interval.ms': self.options.max_poll_interval_ms,
            'session.timeout.ms': self.options.session_timeout_ms, # Often related to poll interval
            # Explicitly enabling auto commit as per requirement
            'enable.auto.commit': True,
            'auto.commit.interval.ms': self.options.auto_commit_interval_ms,
            # Disable the auto.commit.enable config which is deprecated in favor of enable.auto.commit
            # 'auto.commit.enable': self.options.auto_commit_enabled, # REMOVE or ensure it's aligned with enable.auto.commit
        }

        # Ensure auto-commit is truly enabled based on the requirement
        consumer_conf['enable.auto.commit'] = True
        # The options.auto_commit_enabled might still be in your config source,
        # but make sure the consumer_conf explicitly sets enable.auto.commit=True

        self.consumer = None
        try:
            self.consumer = DeserializingConsumer(consumer_conf)
            self.consumer.subscribe([kafka_topic])
            self.logger.info(f"Consumer initialized for topic: {kafka_topic}")
        except Exception as e:
             self.logger.error(f"Failed to initialize Kafka Consumer: {e}")
             self.running = False # Prevent thread from starting listen loop


        # --- Thread Pool for Message Processing ---
        # Using a ThreadPoolExecutor to run handle_message in separate threads
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.options.processing_thread_count
        )
        self._processing_futures = [] # To keep track of submitted tasks


    def run(self):
        """Main thread execution method"""
        if not self.running or self.consumer is None:
            self.logger.error("Consumer or Schema Registry failed to initialize. Exiting run.")
            # Properly shut down executor even if init failed
            self.executor.shutdown(wait=False) # Don't wait if init failed
            return

        # Removed old setup methods (reset_partition_offsets, ignore_messages, use_latest_message)
        # as they conflict with continuous auto-commit and processing.
        # If you still need specific offset handling, use on_assign or manual seek before listen().
        # For typical auto-commit, subscribe() and poll() are sufficient.

        self.listen()

        # Ensure executor is shut down after listen loop exits
        self.executor.shutdown(wait=True, timeout=3) # Wait for processing tasks to complete
        self.logger.info("Processing executor shut down.")

        # Close the consumer
        if self.consumer:
            self.consumer.close()
            self.logger.info(f"Consumer for {self.kafka_topic} closed.")


    def stop(self):
        """Signal the consumer to stop"""
        self.logger.info(f"Stopping consumer for {self.kafka_topic}")
        self.running = False
        # Stopping the thread will eventually cause the listen loop to exit,
        # which then handles executor and consumer shutdown.


    def pause(self, topic: str):
        """Pause consuming from a topic - needs partition assignment"""
        try:
            # Need to poll to ensure partition assignment information is fresh
            # Or preferably wait for assignment via on_assign initially
            self.consumer.poll(0) # Poll non-blocking just to update internal state

            # Get the topic's assigned partitions for this consumer
            assigned_partitions = self.consumer.assignment()
            if not assigned_partitions:
                 self.logger.warning(f"Cannot pause topic {topic}: No partitions currently assigned.")
                 return

            partitions_to_pause = [p for p in assigned_partitions if p.topic == topic]
            if not partitions_to_pause:
                 self.logger.warning(f"Cannot pause topic {topic}: No partitions for this topic currently assigned.")
                 return

            self.consumer.pause(partitions_to_pause)
            self.logger.info(f"Paused consuming from partitions: {partitions_to_pause}")

        except Exception as e:
            self.logger.error(f"Error pausing consumer for topic {topic}: {e}")


    def resume(self, topic: str):
        """Resume consuming from a topic - needs partition assignment"""
        try:
            self.consumer.poll(0) # Poll non-blocking just to update internal state

            assigned_partitions = self.consumer.assignment()
            if not assigned_partitions:
                 self.logger.warning(f"Cannot resume topic {topic}: No partitions currently assigned.")
                 return

            partitions_to_resume = [p for p in assigned_partitions if p.topic == topic]
            if not partitions_to_resume:
                 self.logger.warning(f"Cannot resume topic {topic}: No partitions for this topic currently assigned.")
                 return

            self.consumer.resume(partitions_to_resume)
            self.logger.info(f"Resumed consuming from partitions: {partitions_to_resume}")

        except Exception as e:
            self.logger.error(f"Error resuming consumer for topic {topic}: {e}")


    def listen(self):
        """Listen for messages on the topic and handle them in a thread pool"""
        self.logger.info(f"Starting consumer listen loop for {self.kafka_topic}")
        while self.running:
            try:
                # Poll for a message. Use a reasonable timeout (e.g., 1 second).
                # This keeps the consumer thread active and responsive to signals/rebalances.
                msg = self.consumer.poll(timeout=1.0)

                # Handle poll results
                if msg is None:
                    # No message received within timeout, check for completed processing tasks
                    self._check_processing_futures()
                    continue # Continue polling

                if msg.error():
                    # Handle Kafka errors
                    error_code = msg.error().code()
                    if error_code == KafkaError._PARTITION_EOF:
                        # End of partition event - normal, continue polling
                        self.logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
                        pass # Or log at debug level if needed
                    elif error_code == KafkaError._MAX_POLL_EXCEEDED:
                         # This should be rare with offloaded processing.
                         # The consumer will attempt to rejoin automatically on the next poll().
                         self.logger.error(
                            f"MAX_POLL_EXCEEDED error: {msg.error()}. "
                            "This indicates the consumer thread was blocked for too long. "
                            "Ensure poll() is called frequently. Consumer will attempt to rejoin."
                         )
                         # No extra poll() needed here, the loop continues.
                    elif error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        self.logger.error(f"Kafka error: Topic or Partition unknown - {msg.error()}")
                        # Depending on severity, you might break or retry connection
                        break # Example: Stop loop on fatal error
                    else:
                        # Handle other non-fatal or fatal Kafka errors
                        self.logger.error(f"Kafka error: {msg.error()}")
                        # Decide if this error should stop the consumer or is recoverable
                        # break # Example: Stop loop on other errors

                else:
                    # Valid message received - hand it off to the thread pool
                    self.logger.debug(f"Received message: topic={msg.topic()}, partition={msg.partition()}, offset={msg.offset()}")
                    # Submit the message handling to the executor
                    # Pass message value and topic to the handler
                    future = self.executor.submit(self._handle_message_safe, msg.value(), msg.topic())
                    self._processing_futures.append(future)
                    # Clean up completed futures periodically to prevent list growth
                    self._check_processing_futures()

            except Exception as e:
                # Catch unexpected exceptions in the polling loop
                self.logger.error(f"An unexpected exception occurred in consumer loop: {e}", exc_info=True)
                self.running = False # Stop the loop on unexpected errors


    def _handle_message_safe(self, value, topic):
        """Wrapper around the user's handle_message callback to catch exceptions"""
        try:
            self._handle_message_callback(value, topic)
        except Exception as e:
            self.logger.error(f"Exception occurred in message handler for topic {topic}: {e}", exc_info=True)
            # Decide how to handle exceptions during processing.
            # You might log, send to a dead-letter queue, etc.
            # Raising the exception here would be caught by check_processing_futures.


    def _check_processing_futures(self):
        """Checks completed futures for exceptions and cleans up the list"""
        # Iterate over a copy because we might remove elements
        completed_futures = [f for f in self._processing_futures if f.done()]
        for future in completed_futures:
            self._processing_futures.remove(future)
            try:
                # Calling result() will raise any exception that occurred in the worker thread
                future.result()
            except Exception as e:
                # The exception was already logged in _handle_message_safe,
                # but you could add more logging or error handling here
                self.logger.error(f"Exception in processing thread was propagated: {e}")
                # Decide if an exception in a worker should stop the consumer
                # self.running = False # Example: uncomment to stop consumer on worker error


# Example Usage (assuming you have a main part of your script)
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)

    # Your actual handle_message function
    def my_message_handler(msg_value, topic_name):
        print(f"Handling message in worker for {topic_name}: {msg_value}")
        # Simulate processing time
        import random
        import time
        process_time = random.uniform(0.1, 5) # Simulate variable processing times
        # process_time = random.uniform(10, 20) # Uncomment to potentially test timeouts if not offloading properly
        print(f"Worker for {topic_name} simulating {process_time:.2f} seconds work...")
        time.sleep(process_time)
        print(f"Worker for {topic_name} finished.")
        # If process_time was consistently > max.poll.interval.ms AND processing wasn't offloaded,
        # you would get MAX_POLL_EXCEEDED. With offloading, this simulation doesn't block the consumer thread.


    # Create options
    options = TestBedOptions(
         kafka_host="localhost:9092", # Replace with your broker address
         schema_registry="localhost:8081", # Replace with your SR address
         consumer_group="my_threaded_avro_consumer",
         max_poll_interval_ms=300000, # 5 minutes (generous with offloading)
         session_timeout_ms=45000,   # 45 seconds
         auto_commit_enabled=True,   # Explicitly True as per requirement
         auto_commit_interval_ms=5000, # Auto-commit every 5 seconds
         processing_thread_count=10 # Use 10 worker threads
    )

    kafka_topic = "your_avro_topic" # Replace with your topic name

    # Create and start the consumer manager
    consumer_manager = ConsumerManager(options, kafka_topic, my_message_handler)

    if consumer_manager.running: # Check if initialization was successful
        try:
            consumer_manager.start() # Start the consumer thread
            print("Consumer thread started. Press Ctrl+C to stop.")
            # Keep the main thread alive
            while consumer_manager.is_alive():
                sleep(1)
        except KeyboardInterrupt:
            print("\nCtrl+C detected. Stopping consumer...")
        finally:
            consumer_manager.stop() # Signal the consumer thread to stop
            consumer_manager.join() # Wait for the consumer thread to finish
            print("Consumer thread stopped.")
    else:
        print("Consumer Manager failed to initialize.")
