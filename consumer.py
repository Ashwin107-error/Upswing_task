from celery import Celery
import json
import pytz
# import pika
from pymongo import MongoClient
from datetime import datetime
from flask import Flask, request, jsonify

app = Celery('tasks', broker='pyamqp://guest@localhost//')


flask_app = Flask(__name__)

@app.task(name='tasks.message_processing')
def message_processing(message):
    try:
        mongo_host = 'localhost'
        mongo_port = 27017
        mongo_db = 'mqtt_data'
        mongo_collection = 'messages'
        client = MongoClient(mongo_host, mongo_port)
        db = client[mongo_db]
        collection = db[mongo_collection]

        message = json.loads(message)
        message['timestamp'] = datetime.fromisoformat(message['timestamp'])
        collection.insert_one(message)
        print(f"Processed message: {message}")
    except Exception as e:
        return e



@flask_app.route('/status_count', methods=['GET'])
def get_status_count():

    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    if not start_time or not end_time:
        return jsonify({"error": "start_time and end_time parameters are required"}), 400

    try:
        start_time = datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')

        start_time = start_time.replace(tzinfo=pytz.UTC).isoformat()
        end_time = end_time.replace(tzinfo=pytz.UTC).isoformat()
    except ValueError:
        return jsonify({"error": "Invalid date format. Use ISO format"}), 400
    
    mongo_host = 'localhost'
    mongo_port = 27017
    mongo_db = 'mqtt_data'
    mongo_collection = 'messages'

    client = MongoClient(mongo_host, mongo_port)
    db = client[mongo_db]
    collection = db[mongo_collection]

    pipeline = [
        {"$match": {"timestamp": {"$gte":datetime.fromisoformat(start_time), "$lte": datetime.fromisoformat(end_time)}}},
        {"$group": {"_id": "$status", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    status_count = {item['_id']: item['count'] for item in result}

    return jsonify(status_count)

if __name__ == '__main__':
    flask_app.run(port=5000)

