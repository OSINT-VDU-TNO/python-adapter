import copy
import json
import time
from datetime import datetime
from enum import Enum

from .producer_manager import ProducerManager
from ..options.test_bed_options import TestBedOptions


class BColors:
    OKBLUE = '\033[94m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


def current_milli_time():
    return round(time.time() * 1000)


def timestamp():
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')


def LogLevelToType(level):
    if level == LogLevel.Sill:
        return 'SILLY'
    elif level == LogLevel.Debug:
        return 'DEBUG'
    elif level == LogLevel.Info:
        return 'INFO'
    elif level == LogLevel.Warn:
        return 'WARN'
    elif level == LogLevel.Error:
        return 'ERROR'
    elif level == LogLevel.Critical:
        return 'CRITICAL'


class LogLevel(Enum):
    Sill = 0,
    Debug = 1,
    Info = 2,
    Warn = 3,
    Error = 4,
    Critical = 5


class LogManager:
    def __init__(self, options: TestBedOptions, kafka_topic='system_logging'):
        self.options = copy.deepcopy(options)
        self.options.string_key_type = 'group_id'

        self.kafka_log_producer = ProducerManager(options=self.options, kafka_topic=kafka_topic)

    def sill(self, msg):
        self.log(LogLevel.Sill, msg)

    def debug(self, msg):
        self.log(LogLevel.Debug, msg)

    def info(self, msg):
        self.log(LogLevel.Info, msg)

    def warn(self, msg):
        self.log(LogLevel.Warn, msg)

    def warning(self, msg):
        self.log(LogLevel.Warn, msg)

    def error(self, msg):
        self.log(LogLevel.Error, msg)

    def critical(self, msg):
        self.log(LogLevel.Critical, msg)

    def log(self, level: LogLevel, msg):
        if not isinstance(msg, str):
            try:
                msg = json.dumps(msg)
            except:
                msg = str(msg)

        # Send to console
        if level == LogLevel.Sill:
            print(f'{BColors.OKBLUE}{timestamp()}: Silly: {msg}{BColors.ENDC}')
        elif level == LogLevel.Debug:
            print(f'{BColors.OKBLUE}{timestamp()}: Debug: {msg}{BColors.ENDC}')
        elif level == LogLevel.Info:
            print(f'{BColors.OKBLUE}{timestamp()}: Info: {msg}{BColors.ENDC}')
        elif level == LogLevel.Warn:
            print(f'{BColors.WARNING}{timestamp()}: Warning: {msg}{BColors.ENDC}')
        elif level == LogLevel.Error:
            print(f'{BColors.FAIL}{timestamp()}: Error: {msg}{BColors.ENDC}')
        elif level == LogLevel.Critical:
            print(f'{BColors.FAIL}{timestamp()}: Critical: {msg}{BColors.ENDC}')

        if level == LogLevel.Error or level == LogLevel.Critical or level == LogLevel.Warn:
            # Send to Kafka
            payload = {
                "id": self.options.consumer_group,
                "level": LogLevelToType(level),
                "dateTimeSent": current_milli_time(),
                "log": msg
            }
            message = [payload]
            try:
                self.kafka_log_producer.send_messages(message)
            except Exception as e:
                print(f'Failed to send message to kafka: {e}')
