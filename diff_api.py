import threading
import queue
import time
import os
print(os.getenv('PRIVATE_KEY'))
import redis
from flask import Flask, request, jsonify
from azure.cosmos import CosmosClient
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)

# Cosmos DB Connection credentials
url = "https://dsdproject.documents.azure.com:443/"
key = os.getenv('PRIVATE_KEY')

client = CosmosClient(url, credential=key)
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

# Connect to your database and container
database_name = "ToDoList"
container_name = "Items"
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)
request_queue = queue.Queue()
queue_lock = threading.Lock()
def worker():
    while True:
        try:
            request_id, response_queue, query = request_queue.get(timeout=5)
            print(f"Processing request {request_id}")
            time.sleep(3)
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


def construct_query(column, value, match_type):
    if match_type == 'exact':
        return f"SELECT * FROM c WHERE c.{column} = '{value}'"
    elif match_type == 'partial':
        return f"SELECT * FROM c WHERE CONTAINS(c.{column}, '{value}')"
    else:
        return None


def get_cache_key(query, match_type):
    # Generate a unique cache key based on the query and match type
    return f"{query}_{match_type}"

def get_cached_result(query, match_type):
    # Create a unique cache key
    cache_key = get_cache_key(query, match_type)

    # Check if the result is in the cache
    cached_result = redis_client.get(cache_key)
    if cached_result:
        print(f"Cache hit for {cache_key}")
        return eval(cached_result)  # Evaluate the cached result (stored as string list)

    # If not in cache, query Cosmos DB and cache the result
    print(f"Cache miss for {cache_key}, querying Cosmos DB...")
    request_id = str(time.time())
    response_queue = queue.Queue()

    with queue_lock:
        request_queue.put((request_id, response_queue, query))
        print(f"Current Queue (after adding): {list(request_queue.queue)}")

    try:
        result = response_queue.get(timeout=30)
        # Cache the result for future requests
        redis_client.set(cache_key, str(result), ex=60 * 60)  # Cache for 1 hour
        return result
    except queue.Empty:
        return {"error": "Request timed out"}, 504

@app.route('/search/title', methods=['GET'])
def search_title():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str) 
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('title', query_value, match_type)
    return get_cached_result(query, match_type)

@app.route('/search/abstract', methods=['GET'])
def search_abstract():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('abstract', query_value, match_type)
    return get_cached_result(query, match_type)


@app.route('/search/url', methods=['GET'])
def search_url():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('url', query_value, match_type)
    return get_cached_result(query, match_type)

@app.route('/search/body_text', methods=['GET'])
def search_body_text():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('body_text', query_value, match_type)
    return get_cached_result(query, match_type)

@app.route('/search/body_html', methods=['GET'])
def search_body_html():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('body_html', query_value, match_type)
    return get_cached_result(query, match_type)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=50001, debug=True)
