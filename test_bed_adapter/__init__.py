import logging
from typing import Optional

# No need to import SchemaRegistryClient or SchemaRegistryError here anymore


from .kafka.heartbeat_manager import HeartbeatManager
from .options.test_bed_options import TestBedOptions

# Import other managers here if they exist
# from .kafka.consumer_manager import ConsumerManager
# from .kafka.producer_manager import ProducerManager as MainProducerManager


class TestBedAdapter:
    def __init__(self, test_bed_options: TestBedOptions):
        self.logger = logging.getLogger(__name__)
        self.test_bed_options = test_bed_options
        self.heartbeat_manager: Optional[HeartbeatManager] = None
        try:
            self.test_bed_options.validate_options()
        except ValueError as e:
            self.logger.error(f"Configuration validation failed: {e}", exc_info=True)
            raise  # Re-raise the critical error

    def initialize(self):
        """Initializes the components of the adapter."""
        self.logger.info("Initializing python adapter")

        # Initialize heartbeat manager (includes the interval check)
        self.init_and_start_heartbeat()

        self.logger.info("Python adapter initialization complete.")

    def init_and_start_heartbeat(self):
        """Initializes and starts the heartbeat manager if configured."""
        heartbeat_topic = "system_heartbeat"

        # --- Check heartbeat_interval first ---
        heartbeat_interval = self.test_bed_options.heartbeat_interval
        if heartbeat_interval is not None and heartbeat_interval <= 0:
            self.logger.info(
                f"Heartbeat disabled as heartbeat_interval is {heartbeat_interval} <= 0."
            )
            return  # Do not proceed with heartbeat setup

        # --- Initialize HeartbeatManager if interval is valid ---
        # The HeartbeatManager will create its own ProducerManager, which will handle
        # checking for the schema and topic existence internally.
        # If the ProducerManager fails to initialize (e.g., schema not found),
        # the HeartbeatManager will have a non-functional producer and won't send messages.
        try:
            self.logger.info(
                f"Heartbeat enabled (interval: {heartbeat_interval}s). Initializing HeartbeatManager..."
            )
            self.heartbeat_manager = HeartbeatManager(
                options=self.test_bed_options, kafka_topic=heartbeat_topic
            )

            # Check if the producer inside HeartbeatManager initialized successfully.
            # This implicitly checks if the topic/schema were available during ProducerManager init.
            if (
                self.heartbeat_manager.kafka_heartbeat_producer is not None
                and self.heartbeat_manager.kafka_heartbeat_producer.producer is not None
            ):
                self.heartbeat_manager.start()
                self.logger.info("Heartbeat thread started.")
            else:
                # The ProducerManager logs the specific reason (e.g., schema not found, SR error)
                self.logger.error(
                    "Heartbeat ProducerManager failed to initialize. Heartbeat thread will not start and messages will not be sent."
                )
                self.heartbeat_manager = (
                    None  # Clear the reference if it's not functional
                )

        except Exception as e:
            self.logger.error(
                f"Error initializing or starting HeartbeatManager: {e}", exc_info=True
            )
            self.heartbeat_manager = None  # Ensure manager is None on error

    def stop(self):
        """Stops the adapter components."""
        self.logger.info("Stopping python adapter")

        # Stop the heartbeat thread and wait for it to finish
        if self.heartbeat_manager is not None:
            self.logger.info("Stopping heartbeat manager...")
            self.heartbeat_manager.stop()  # Signal the thread's run loop to exit
            self.heartbeat_manager.join()  # Wait for the run method to finish
            self.logger.info("Heartbeat manager stopped.")

        self.logger.info("Python adapter stopped.")
