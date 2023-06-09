# NAAS-PYTHON-KAFKA

This is the Kafka adapter for Python: it allows you to easily connect Python 
services to Apache Kafka via Python.

The implementation is a wrapper around [Confluent-Kafka-Python](https://github.com/confluentinc/confluent-kafka-python):

- AVRO schema's and messages: both key's and values should have a schema.
as explained [here](https://github.com/DRIVER-EU/avro-schemas).
- Kafka consumer and producer for the test-bed topics.
- Management
  - Heartbeat (topic: system-heartbeat), so you know which clients are online.
  Each time the test-bed-adapter is executed, it starts a heartbeat process to notify
  the its activity to other clients.

## Installation

You need to install [Python 3+](https://www.python.org/).

To run the examples you will need to install the dependencies specified on the file [requirements.txt](https://github.com/DRIVER-EU/python-test-bed-adapter/blob/master/requirements.txt).

For that, run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt # Or instead of `pip`, use `pip3`
```

from the project folder.

## Examples and usage

- url_producer: creates a message with 4 URLs to RSS feeds on the topic ('system_rss_urls')
- rss_producer: listens to url messages ('system_rss_urls') and produces RSS messages ('system_rss_urls')
- rss_consumer: listens to RSS messages ('system_rss_urls') and prints them to console.
