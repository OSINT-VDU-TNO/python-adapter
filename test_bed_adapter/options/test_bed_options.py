import logging
from typing import Optional

# from typing import List, Optional # Add these imports if you uncomment list/optional attributes


class TestBedOptions:
    """
    Configuration options for the TestBed Adapter.

    Attributes:
        consumer_group (str | None): Group ID that this adapter should join.
        kafka_host (str | None): Uri for the Kafka broker, e.g. broker:3501. Required.
        schema_registry (str): Uri for the schema registry, e.g. schema_registry:3502.
        message_max_bytes (int): Maximum size for a single Kafka message in bytes.
        partitioner (str): Partitioner type for producer. Values: random, consistent, ...
        string_based_keys (bool): Use string based keys for the producer.
        string_key_type (str): If string_based_keys is true, this is the type of the key. Values: id, group_id.
        max_poll_interval_ms (int): Maximum time allowed between calls to poll() in milliseconds.
        session_timeout_ms (int): Consumer session timeout in milliseconds.
        auto_commit_interval_ms (int): Consumer automatic offset commit interval in milliseconds.
        processing_thread_count (int): Number of processing threads (if applicable).
        offset_type (str): Consumer offset reset policy. Values: earliest, latest, error.
        ignore_timeout (bool | None): Ignore messages that for timeout (Unclear purpose - add doc).
        use_latest (bool): If true, use the latest message (Unclear purpose - add doc).
        heartbeat_interval (int): Interval between two heartbeat messages in seconds. Set to 0 or negative to disable.
        # Add type hints and docstrings for other options as needed
        # use_ssl (bool): If set true, use SSL.
        # ca_file (str): Path to trusted CA certificate.
        # cert_file (str | None): Path to client certificate.
        # key_file (str | None): Path to client private-key file.
        # password_private_key (str | None): Password for private key.
        # consume (List[str]): Topics you want to consume.
        # produce (List[str]): Topics you want to produce.
    """

    def __init__(self, dictionary: dict):
        # Initialize with default values and type hints
        self.consumer_group: Optional[str] = None
        self.kafka_host: str = (
            "localhost:3501"  # Mark as Optional as it's checked in validate_options
        )
        self.schema_registry: str = "http://localhost:3502"
        self.message_max_bytes: int = 10000000
        self.partitioner: str = "random"
        self.string_based_keys: bool = True
        self.string_key_type: str = "id"
        self.max_poll_interval_ms: int = 300000
        self.session_timeout_ms: int = 90000
        self.offset_type: str = "latest"
        self.heartbeat_interval: int = (
            0  # Default is 0 seconds, equivalent to no heartbeat
        )

        # Override default values with values from the dictionary
        for key, value in dictionary.items():  # Use .items() for clarity
            if hasattr(self, key):  # Check if the option exists in the defaults
                # Optional: Add type checking here if needed
                setattr(self, key, value)
            else:
                logging.warning(f"Unknown option provided in dictionary: '{key}'")

    def validate_options(self):
        """
        Validates the configured options. Raises ValueError if options are invalid.
        """
        if not self.kafka_host:
            raise ValueError("kafka_host option must be provided.")
        if not self.schema_registry:
            raise ValueError("schema_registry option must be provided.")

        # Example: Check partitioner value is valid
        valid_partitioners = [
            "random",
            "consistent",
            "consistent_random",
            "murmur2",
            "murmur2_random",
            "fnv1a",
            "fnv1a_random",
        ]
        if self.partitioner not in valid_partitioners:
            raise ValueError(
                f"Invalid partitioner value: {self.partitioner}. Must be one of {valid_partitioners}"
            )

        logging.info("TestBedOptions validated successfully.")

    # Assuming get_options_from_file would load a dictionary and pass it to __init__
    # def get_options_from_file(self, file_path):
    #     """Loads options from a file (e.g., JSON, YAML)."""
    #     # Implement file loading logic here
    #     options_dict = {} # Load from file
    #     self.__init__(options_dict) # Re-initialize with loaded options
