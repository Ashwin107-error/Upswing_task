from celery import Celery
import json
import random
import time
from datetime import datetime
from consumer import message_processing

def message():
    payload = {
            'status':random.randint(0,6),
            'timestamp':datetime.now().isoformat()
    }
    return json.dumps(payload)

def scheduling():
    while True:
        messages = message()
        print(messages)
        message_processing.apply_async(args = (messages,), queue = "mqtt_queue")
        time.sleep(1)

if __name__ == '__main__':
    try:
        scheduling()
    except KeyboardInterrupt:
        print("Stopping worker...")

