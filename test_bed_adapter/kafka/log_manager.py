from ..options.test_bed_options import TestBedOptions
from .producer_manager import ProducerManager
from ..utils.helpers import Helpers
from enum import Enum
import json
from datetime import datetime
import time


class bcolors:
    OKBLUE = '\033[94m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'


def current_milli_time():
    return round(time.time() * 1000)


def timestamp():
    return datetime.today().strftime('%Y-%m-%d %H:%M:%S')


def LogLevelToType(level):
    if(level == LogLevel.Sill):
        return 'SILLY'
    elif(level == LogLevel.Debug):
        return 'DEBUG'
    elif(level == LogLevel.Info):
        return 'INFO'
    elif(level == LogLevel.Warn):
        return 'WARN'
    elif(level == LogLevel.Error):
        return 'ERROR'
    elif(level == LogLevel.Critical):
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
        self.options = options
        self.helper = Helpers()
        self.interval_thread = {}

        self.kafka_log_producer = ProducerManager(
            options=self.options, kafka_topic=kafka_topic)

    def sill(self, msg):
        self.log(LogLevel.Sill, msg)

    def debug(self, msg):
        self.log(LogLevel.Debug, msg)

    def info(self, msg):
        self.log(LogLevel.Info, msg)

    def warn(self, msg):
        self.log(LogLevel.Warn, msg)

    def error(self, msg):
        self.log(LogLevel.Error, msg)

    def critical(self, msg):
        self.log(LogLevel.Critical, msg)

    def log(self, level: LogLevel, msg):
        if(not isinstance(msg, str)):
            msg = json.dumps(msg)

        # Send to console
        if(level == LogLevel.Sill):
            print(f'{bcolors.OKBLUE}{timestamp()}: Silly: {msg}{bcolors.ENDC}')
        elif(level == LogLevel.Debug):
            print(f'{bcolors.OKBLUE}{timestamp()}: Debug: {msg}{bcolors.ENDC}')
        elif(level == LogLevel.Info):
            print(f'{bcolors.OKBLUE}{timestamp()}: Info: {msg}{bcolors.ENDC}')
        elif(level == LogLevel.Warn):
            print(f'{bcolors.WARNING}{timestamp()}: Warning: {msg}{bcolors.ENDC}')
        elif(level == LogLevel.Error):
            print(f'{bcolors.FAIL}{timestamp()}: Error: {msg}{bcolors.ENDC}')
        elif(level == LogLevel.Critical):
            print(f'{bcolors.FAIL}{timestamp()}: Critical: {msg}{bcolors.ENDC}')

        # Send to Kafka
        payload = {
            "id": self.options.consumer_group,
            "level": LogLevelToType(level),
            "dateTimeSent": current_milli_time(),
            "log": msg
        }
        message = [payload]
        self.kafka_log_producer.send_messages(message)
