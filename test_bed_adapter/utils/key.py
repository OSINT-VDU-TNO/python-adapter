import time
from datetime import datetime
from uuid import uuid4


def generate_key(message, options):
    """Generate a key for the Kafka message."""
    try:
        if not (options.string_based_keys or str(options.string_based_keys).lower().strip() == "true"):
            return _generate_edxl_key(options)
        if options.string_key_type == 'id':
            return message['id']
        else:
            return options.consumer_group
    except:
        return _generate_edxl_key(options)


def _generate_edxl_key(options):
    """Generate a EDXL key for the Kafka message."""
    date = datetime.utcnow()
    date_ms = int(time.mktime(date.timetuple())) * 1000
    return {"distributionID": str(uuid4()), "senderID": options.consumer_group,
            "dateTimeSent": date_ms, "dateTimeExpires": 0,
            "distributionStatus": "Test", "distributionKind": "Unknown"}
