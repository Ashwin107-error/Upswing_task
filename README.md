1. clone the repository:

2. Create and activate Virtual env:
  python3 -m venv venv
  source venv/bin/activate

3. Install Required packages:
  pip install -r requirements.txt

4. Ensure RMQ and mongodb are running
  
5. Start the Celery worker:
   celery -A consumer worker --loglevel=info -Q mqtt_queue
 
6. Run the worker script:
   python worker.py

7. Run the Flask application:
   python consumer.py

8. Curl request for api endpoint:
   curl "http://127.0.0.1:5000/status_count?start_time=2024-06-25 00:00:00&end_time=2024-06-26 00:00:00"
   Endpoint: /status_count
   Method: GET