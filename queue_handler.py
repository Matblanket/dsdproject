import threading
import queue
import time
from flask import Flask, request, jsonify
from azure.cosmos import CosmosClient

app = Flask(__name__)

# Cosmos DB Connection credentials
url = "https://dsdproject.documents.azure.com:443/"
key = ""
client = CosmosClient(url, credential=key)
database_name = "ToDoList"
container_name = "Items"
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)
request_queue = queue.Queue()
queue_lock = threading.Lock()

# Worker function to process the requests in the queue
def worker():
    while True:
        try:
            
            request_id, response_queue = request_queue.get(timeout=5)  # Timeout after 5 seconds if queue is empty
            print(f"Processing request {request_id}")
            time.sleep(3)  
            query = "SELECT * FROM c WHERE c.abstract='= Preface ='"
            items = container.query_items(query=query, enable_cross_partition_query=True)
            result = [item for item in items]
            print(f"[Worker Thread] Putting response for request {request_id} into the response queue")
            response_queue.put(result)
            request_queue.task_done()
            print(f"[Worker Thread] Current Queue (after processing): {list(request_queue.queue)}")
        except queue.Empty:
            pass

worker_thread = threading.Thread(target=worker, daemon=True)
worker_thread.start()

@app.route('/query', methods=['GET'])
def query_cosmosdb():
    request_id = str(time.time()) 
    print(f"[Main Thread] Creating a queue for request {request_id}")
    response_queue = queue.Queue()
    with queue_lock:

        print(f"[Main Thread] Adding request {request_id} to the request queue")
        request_queue.put((request_id, response_queue))
        print(f"[Main Thread] Current Queue (after adding): {list(request_queue.queue)}")

    try:
        print(f"[Main Thread] Waiting for response to request {request_id}")
        result = response_queue.get(timeout=30)  # Wait for up to 30 seconds for a response
        print(f"[Main Thread] Received response for request {request_id}")
        return jsonify(result)
    except queue.Empty:
        print("Request timedout")
        return jsonify({"error": "Request timed out"}), 504

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=50001, debug=True)

