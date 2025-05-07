import logging
from threading import Thread, Event, Lock
from time import sleep
import concurrent.futures

from confluent_kafka import DeserializingConsumer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ..options.test_bed_options import TestBedOptions

class ConsumerManager(Thread):
    def __init__(self, options: TestBedOptions, kafka_topic, handle_message):
        super().__init__()
        self.logger = logging.getLogger(__name__)
        self.running = True
        self.daemon = True  # Allow program to exit even if this thread is running
        self.options = options
        self._handle_message_callback = handle_message
        self.kafka_topic = kafka_topic
        
        # Control flow events
        self._stop_event = Event()
        self._pause_event = Event()
        
        # Processing state control
        self._processing_lock = Lock()  # Used to protect access to processing state
        self._currently_processing = False
        self._heartbeat_enabled = True
        
        # --- Schema Registry and Deserializer Setup ---
        try:
            sr_conf = {'url': self.options.schema_registry}
            schema_registry_client = SchemaRegistryClient(sr_conf)
            self.avro_deserializer = AvroDeserializer(schema_registry_client)
        except Exception as e:
            self.logger.error(f"Failed to initialize Schema Registry: {e}")
            self.running = False  # Prevent thread from starting

        # --- Kafka Consumer Configuration ---
        consumer_conf = {
            'bootstrap.servers': self.options.kafka_host,
            'key.deserializer': self.avro_deserializer,
            'value.deserializer': self.avro_deserializer,
            'group.id': self.options.consumer_group,
            'message.max.bytes': self.options.message_max_bytes,
            'auto.offset.reset': self.options.offset_type,
            'max.poll.interval.ms': self.options.max_poll_interval_ms,
            'session.timeout.ms': self.options.session_timeout_ms,
            
            # Critical: disable auto-commit to ensure exactly-once processing
            'enable.auto.commit': False,
        }

        self.consumer = None
        try:
            self.consumer = DeserializingConsumer(consumer_conf)
            self.consumer.subscribe([kafka_topic])
            self.logger.info(f"Sequential Consumer initialized for topic: {kafka_topic}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka Consumer: {e}")
            self.running = False
            
        # Thread pool for the single worker that processes messages
        # This allows us to process messages outside of the main consumer thread
        # while still maintaining sequential processing
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self._current_task = None
        
        # Set up the heartbeat thread
        self._heartbeat_thread = Thread(target=self._heartbeat_loop, daemon=True)

    def run(self):
        """Main thread execution method"""
        if not self.running or self.consumer is None:
            self.logger.error("Consumer failed to initialize. Exiting run.")
            if self.executor:
                self.executor.shutdown(wait=False)
            return
            
        # Start the heartbeat thread
        self._heartbeat_thread.start()
        
        # Start the main listening loop
        self.listen()
        
        # Shutdown and cleanup
        self._heartbeat_enabled = False  # Signal heartbeat thread to stop
        if self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=2.0)
            
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
        
    def _heartbeat_loop(self):
        """
        Separate thread that calls poll(0) periodically to maintain the consumer's 
        membership in the consumer group even during long-running message processing.
        """
        self.logger.info("Heartbeat thread started")
        
        # Calculate a reasonable heartbeat interval 
        # (typically 1/3 of session.timeout.ms but not more often than every second)
        heartbeat_interval = min(max(1.0, self.options.session_timeout_ms / 3000.0), 5.0)
        self.logger.info(f"Heartbeat interval set to {heartbeat_interval:.1f} seconds")
        
        while self._heartbeat_enabled and not self._stop_event.is_set():
            try:
                # Only send heartbeats when we're processing a message
                with self._processing_lock:
                    if self._currently_processing and self.consumer:
                        # Poll with 0 timeout to just handle broker communication without blocking
                        self.consumer.poll(0)
                        self.logger.debug("Heartbeat poll sent")
            except Exception as e:
                self.logger.error(f"Error in heartbeat thread: {e}")
                
            # Sleep for the heartbeat interval
            sleep(heartbeat_interval)
            
        self.logger.info("Heartbeat thread stopped")

    def listen(self):
        """Listen for messages one at a time, processing each in a separate thread"""
        self.logger.info(f"Starting sequential consumer loop for {self.kafka_topic}")
        
        while not self._stop_event.is_set() and self.running:
            # Check if we should pause
            if self._pause_event.is_set():
                sleep(0.5)
                continue
                
            # Check if we're still processing the last message
            if self._current_task and not self._current_task.done():
                # The previous message is still being processed
                # Poll with timeout=0 to maintain liveness without fetching new messages
                self.consumer.poll(0)
                sleep(0.1)
                continue
                
            try:
                # Poll for a message with normal timeout
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue  # No message received, poll again
                
                if msg.error():
                    self._handle_kafka_error(msg)
                    continue
                
                # Got a valid message - process it in the worker thread
                self._process_message_async(msg)
                
            except Exception as e:
                # Catch unexpected exceptions in the polling loop
                self.logger.error(f"An unexpected exception occurred in consumer loop: {e}", exc_info=True)
                if self.running:
                    self.running = False  # Stop on unexpected errors

    def _process_message_async(self, msg):
        """Submit a message for processing in the worker thread"""
        # Store message context for committing after processing
        topic = msg.topic()
        partition = msg.partition()
        offset = msg.offset()
        value = msg.value()
        
        self.logger.info(f"Submitting message for processing: topic={topic}, partition={partition}, offset={offset}")
        
        # Mark that we're starting to process a message
        with self._processing_lock:
            self._currently_processing = True
            
        # Submit the task to our single-worker thread pool
        self._current_task = self.executor.submit(
            self._process_single_message, 
            value, 
            topic, 
            msg
        )
        
        # Add a callback for when processing completes
        self._current_task.add_done_callback(self._on_message_processed)
            
    def _process_single_message(self, value, topic, msg):
        """Process a single message in the worker thread"""
        try:
            # Call the user's handler
            self.logger.info(f"Processing message from {topic}")
            self._handle_message_callback(value, topic)
            
            # Return the message for committing
            return {'status': 'success', 'msg': msg}
                
        except Exception as e:
            self.logger.error(f"Error processing message: {e}", exc_info=True)
            # Return the message along with error status
            return {'status': 'error', 'msg': msg, 'error': str(e)}
            
    def _on_message_processed(self, future):
        """Callback when message processing completes"""
        try:
            # Get the result
            result = future.result()
            msg = result['msg']
            self.consumer.commit(msg)
            
            if result['status'] == 'success':
                self.logger.info(f"Successfully processed and committed: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
            else:
                self.logger.warning(f"Failed message handled and committed: {msg.topic()}[{msg.partition()}] at offset {msg.offset()}")
                    
        except Exception as e:
            self.logger.error(f"Error in message completion handling: {e}")
            
        finally:
            # Mark that we're done processing
            with self._processing_lock:
                self._currently_processing = False

    def _handle_kafka_error(self, msg):
        """Handle Kafka errors from poll"""
        error_code = msg.error().code()
        if error_code == KafkaError._PARTITION_EOF:
            # End of partition event - normal
            self.logger.debug(f"Reached end of partition: {msg.topic()} [{msg.partition()}]")
        elif error_code == KafkaError._MAX_POLL_EXCEEDED:
            self.logger.error(
                f"MAX_POLL_EXCEEDED error: {msg.error()}. "
                "This indicates the consumer thread was blocked for too long. "
                "Check if heartbeat thread is running properly."
            )
        elif error_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
            self.logger.error(f"Kafka error: Topic or Partition unknown - {msg.error()}")
        else:
            self.logger.error(f"Kafka error: {msg.error()}")


# Example Usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Your message handler function
    def my_message_handler(msg_value, topic_name):
        print(f"Handling message for {topic_name}: {msg_value}")
        # Simulate long processing time
        import time
        print(f"Worker simulating LONG processing time (60 seconds)...")
        time.sleep(60)  # Long processing time (1 minute)
        print(f"Processing finished.")

    # Create options
    options = TestBedOptions(
        kafka_host="localhost:9092",
        schema_registry="localhost:8081",
        consumer_group="my_sequential_avro_consumer",
        max_poll_interval_ms=300000,  # 5 minutes
        session_timeout_ms=45000,     # 45 seconds
        offset_type="earliest"        # Start from earliest available message if no committed offset
    )

    kafka_topic = "your_avro_topic"

    # Create and start the consumer manager
    consumer = ConsumerManager(options, kafka_topic, my_message_handler)

    if consumer.running:  # Check if initialization was successful
        try:
            consumer.start()  # Start the consumer thread
            print("Consumer thread started. Press Ctrl+C to stop.")
            
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