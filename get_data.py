import queue
import threading
import time
from flask import Flask, request, jsonify
from azure.cosmos import CosmosClient
import requests

app = Flask(__name__)

url = "https://dsdproject.documents.azure.com:443/"
key = ""
client = CosmosClient(url, credential=key)

database_name = "ToDoList"
container_name = "Items"
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)
request_queue = queue.Queue()
queue_lock = threading.Lock()

def worker():
    while True:
        try:
            request_id, client_url, column, value, match_type = request_queue.get(timeout=5)  # Timeout after 5 seconds if queue is empty
            print(f"Processing request {request_id}")
            time.sleep(3)  
            query = construct_query(column, value, match_type)
            items = container.query_items(query=query, enable_cross_partition_query=True)
            result = [item for item in items]
            send_response_to_client(client_url, result)
            request_queue.task_done()
        except queue.Empty:
            pass

def construct_query(column, value, match_type):
    if match_type == 'exact':
        return f"SELECT * FROM c WHERE c.{column} = '{value}'"
    elif match_type == 'partial':
        return f"SELECT * FROM c WHERE CONTAINS(c.{column}, '{value}')"
    else:
        return None

def send_response_to_client(client_url, response_data):
    try:
        response = requests.post(client_url, json=response_data)
        print(f"Sent response to client: {client_url}")
    except Exception as e:
        print(f"Error sending response to client: {str(e)}")

worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()

@app.route('/search/<column>', methods=['GET'])
def search(column):
    query_value = request.args.get('value')
    match_type = request.args.get('match_type', default='partial')
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400
    request_id = str(time.time())
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    client_url = f"http://{client_ip}:{request.environ.get('REMOTE_PORT')}/response"
    print(f"Received request {request_id} to search {column}={query_value} from client at {client_url}")
    with queue_lock:
        request_queue.put((request_id, client_url, column, query_value, match_type))

    return jsonify({"message": "Request added to queue. Processing in background."})

@app.route('/response', methods=['POST'])
def receive_response():
    response_data = request.json
    print(f"Received response data: {response_data}")
    return jsonify({"message": "Response received from server container"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=50001, debug=True)

