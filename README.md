# python-test-bed-adapter

This is the test-bed adapter for Python: it allows you to easily connect Python 
services to the Apache Kafka test-bed via Python. 

The implementation is a wrapper around [Pykafka](https://github.com/Parsely/pykafka) 
and [avro-python3](https://avro.apache.org/docs/1.8.2/gettingstartedpython.html) 
offering support for:
- AVRO schema's and messages: both key's and values should have a schema 
as explained [here](https://github.com/DRIVER-EU/avro-schemas).
- Kafka consumer and producer for the test-bed topics.
- Management
  - Heartbeat (topic: connect-status-heartbeat), so you know which clients are online.
  Each time the test-bed-adapter is executed, it starts a heartbeat process to notify
  the its activity to other clients.
  - Configuration (topic: connect-status-configuration), so you can see which 
  topics clients consume and produce.

## Installation
You need to install [Python 3+](https://www.python.org/). 

To run the examples you will need to install the dependencies specified on the file[requirements.txt](https://github.com/DRIVER-EU/python-test-bed-adapter/blob/master/requirements.txt)
For that, run
```pip3 install -r requirements.txt```
from the project folder.
## Examples and usage
- url_producer: creates a message with 4 URLs to RSS feeds on the topic ('system_rss_urls')
- rss_producer: listens to url messages ('system_rss_urls') and produces RSS messages ('system_rss_urls')
- rss_consumer: listens to RSS messages ('system_rss_urls') and prints them to console.
