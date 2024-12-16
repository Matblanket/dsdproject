import threading
import queue
import time
import re
import random
import string
from collections import Counter
from flask import Flask, request, jsonify
from azure.cosmos import CosmosClient
from flask_cors import CORS
from PIL import Image, ImageDraw, ImageFont
import io
import base64
import numpy as np
import hashlib
from io import BytesIO
import uuid



app = Flask(__name__)
CORS(app)
# Cosmos DB Connection credentials
url = "https://dsdproject.documents.azure.com:443/"
key = "LDcZZqVhctW4aImG7LY4itVAB0Leffo9OSr09vW756B6YdFWROUbzEPrwIEjcfOZZKL0dhQ4iE0RACDb7xeJpA=="
client = CosmosClient(url, credential=key)

database_name = "ToDoList"
container_name = "Items"
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)
request_queue = queue.Queue()
queue_lock = threading.Lock()
update_locks = {}

def worker():
    while True:
        try:
            request_id, response_queue, request_data = request_queue.get(timeout=5)  
            request_type = request_data.get('type')  
            print(f"Processing request {request_id} with type: {request_type}")
            
            if request_type == 'query':
                query = request_data.get('query')
                items = container.query_items(query=query, enable_cross_partition_query=True)
                result = [item for item in items]
                print(f"[Worker Thread] Query result for request {request_id}")
                response_queue.put(result)
            
            elif request_type == 'insert':
                book_record = request_data.get('book_record')
                container.upsert_item(book_record)  
                print(f"[Worker Thread] Inserted record {book_record['id']} into DB")
                response_queue.put({"message": "Book added successfully", "id": book_record['id']})
            
            elif request_type == 'update':
                record_id = request_data.get('id')
                column = request_data.get('column')
                new_value = request_data.get('new_value')

                if record_id not in update_locks:
                    update_locks[record_id] = True
                    try:
                        query = f"SELECT * FROM c WHERE c.id = '{record_id}'"
                        items = container.query_items(query=query, enable_cross_partition_query=True)
                        record = next(iter(items), None)

                        if record:
                            record[column] = new_value
                            container.upsert_item(record)  
                            print(f"[Worker Thread] Updated {column} of record {record_id} to {new_value}")
                            response_queue.put({"message": "Record updated successfully", "id": record_id})
                        else:
                            response_queue.put({"message": "Record not found", "id": record_id})

                    except Exception as e:
                        response_queue.put({"message": f"Error updating record: {str(e)}", "id": record_id})

                    finally:
                        del update_locks[record_id]
                else:
                    response_queue.put({"message": "Record is being updated, please try again later", "id": record_id})
            request_queue.task_done()
            print(f"[Worker Thread] Current Queue: {list(request_queue.queue)}")
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

def calculate_word_frequencies(text):
    words = re.findall(r'\w+', text.lower())  
    word_counts = Counter(words)  
    return word_counts

def get_top_n_words(word_counts, n=50):
    return dict(word_counts.most_common(n))  

def hsv_to_rgb(hue, saturation, value):
    h = hue * 360  
    s = saturation
    v = value
    c = v * s
    x = c * (1 - abs(((h / 60) % 2) - 1))
    m = v - c

    if 0 <= h < 60:
        r, g, b = c, x, 0
    elif 60 <= h < 120:
        r, g, b = x, c, 0
    elif 120 <= h < 180:
        r, g, b = 0, c, x
    elif 180 <= h < 240:
        r, g, b = 0, x, c
    elif 240 <= h < 300:
        r, g, b = x, 0, c
    else:
        r, g, b = c, 0, x

    r = int((r + m) * 255)
    g = int((g + m) * 255)
    b = int((b + m) * 255)

    return r, g, b

def normalize_frequencies(word_counts):
    max_frequency = max(word_counts.values())
    for word in word_counts:
        word_counts[word] = word_counts[word] / max_frequency 
    return word_counts

def generate_word_signatures(text):
    word_counts = calculate_word_frequencies(text)
    normalized_word_counts = normalize_frequencies(word_counts)
    
    signatures = {}
    for word, frequency in normalized_word_counts.items():
        signature = generate_word_signature(word, frequency)
        signatures[word] = signature
    
    return signatures

def map_frequency_to_hue(frequency):
    hue = frequency  
    return hue

def generate_word_signature(word, frequency):
    hue = map_frequency_to_hue(frequency)
    r, g, b = hsv_to_rgb(hue, 1.0, 1.0)

    return {
        "word": word,
        "frequency": frequency,
        "hue": hue,
        "rgb_color": (r, g, b)
    }

def create_hue_based_image(signatures, image_size=(800, 600)):
    img = Image.new('RGB', image_size, color=(255, 255, 255))
    pixels = np.array(img)  

    word_list = list(signatures.values())  
    grid_size = int(np.sqrt(len(word_list)))
    idx = 0
    for i in range(grid_size):
        for j in range(grid_size):
            if idx < len(word_list):
                word_signature = word_list[idx]
                rgb = word_signature['rgb_color']
                pixels[i * 50:(i + 1) * 50, j * 50:(j + 1) * 50] = rgb  
                idx += 1
    img = Image.fromarray(pixels)
    return img

@app.route('/search/title', methods=['GET'])
def search_title():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str) 
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('title', query_value, match_type)
    request_id = str(time.time())
    response_queue = queue.Queue()
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
            }))
        print(f"Title api Current Queue (after adding): {list(request_queue.queue)}")
    
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result)
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/search/abstract', methods=['GET'])
def search_abstract():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('abstract', query_value, match_type)
    
    request_id = str(time.time())
    response_queue = queue.Queue()
    
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
            }))
        print(f"abstract api Current Queue (after adding): {list(request_queue.queue)}")
    
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result)
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/search/url', methods=['GET'])
def search_url():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('url', query_value, match_type)
    
    request_id = str(time.time())
    response_queue = queue.Queue()
    
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
        }))
        print(f"URL api Current Queue (after adding): {list(request_queue.queue)}")
    
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result)
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/search/body_text', methods=['GET'])
def search_body_text():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('body_text', query_value, match_type)
    
    request_id = str(time.time())
    response_queue = queue.Queue()
    
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
        }))
        print(f"body text api Current Queue (after adding): {list(request_queue.queue)}")
    
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result)
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/search/body_html', methods=['GET'])
def search_body_html():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)
    
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('body_html', query_value, match_type)
    
    request_id = str(time.time())
    response_queue = queue.Queue()
    
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
        }))
        print(f"body html api Current Queue (after adding): {list(request_queue.queue)}")
    
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result)
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/book/<book_id>', methods=['GET'])
def get_book(book_id):
    query = f"SELECT c.body_text,c.title,c.abstract FROM c WHERE c.id = '{book_id}'"
    request_id = str(time.time())
    response_queue = queue.Queue()
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
        }))
        print(f"body html api Current Queue (after adding): {list(request_queue.queue)}")
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result)
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/search/summary/title', methods=['GET'])
def summarize_book():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)

    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('title', query_value, match_type)
    request_id = str(time.time())
    response_queue = queue.Queue()

    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
            }))
        print(f"Title API Current Queue (after adding): {list(request_queue.queue)}")

    try:
        result = response_queue.get(timeout=30)
        if result:
            word_statistics = {}
            for item in result:
                title = item.get('title', 'N/A')
                body_text = item.get('body_text', '')
                if body_text:
                    word_counts = calculate_word_frequencies(body_text)
                    top_word_counts = get_top_n_words(word_counts)  
                    word_statistics[title] = top_word_counts
                else:
                    word_statistics[title] = {}
            print(word_statistics)
            return jsonify({
                'word_statistics': word_statistics
            })

        else:
            return jsonify({"error": "No results found."}), 404


    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504


@app.route('/search/summary/image', methods=['GET'])
def get_image():
    query_value = request.args.get('value', default=None, type=str)
    match_type = request.args.get('match_type', default='partial', type=str)

    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    query = construct_query('title', query_value, match_type)
    request_id = str(time.time())
    response_queue = queue.Queue()

    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'query',
            'query': query
        }))
        print(f"Title API Current Queue (after adding): {list(request_queue.queue)}")

    try:
        result = response_queue.get(timeout=30)
        if result:
            word_statistics = {}
            for item in result:
                title = item.get('title', 'N/A')
                body_text = item.get('body_text', '')
                if body_text:
                    signatures = generate_word_signatures(body_text)
                    img = create_hue_based_image(signatures)

                else:
                    word_statistics[title] = {}
            buffered = BytesIO()
            img.save(buffered, format="PNG")
            img_str = base64.b64encode(buffered.getvalue()).decode('utf-8')  
            return jsonify({"word_cloud_image": img_str})
        else:
            return jsonify({"error": "No results found."}), 404

    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/add_record', methods=['POST'])
def add_record():
    data = request.get_json()
    title = data.get('title')
    url = data.get('url')
    abstract = data.get('abstract')
    body_text = data.get('body_text')
    body_html = data.get('body_html')
    unique_id = str(uuid.uuid4())
    book_record = {
        'id': unique_id,
        'title': title,
        'url': url,
        'abstract': abstract,
        'body_text': body_text,
        'body_html': body_html
    }
    request_id = str(time.time())
    response_queue = queue.Queue()
    with queue_lock:
        request_queue.put((request_id, response_queue, {
            'type': 'insert',
            'book_record': book_record
            }))
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result), 200
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

@app.route('/update_record', methods=['POST'])
def update_record():
    data = request.get_json()
    record_id = data.get('id')
    column = data.get('column')
    new_value = data.get('new_value')
    if not record_id or not column or not new_value:
        return jsonify({"success": False, "message": "Missing required fields"}), 400
    response_queue = queue.Queue()
    with queue_lock:
        request_data = {
            'type': 'update',
            'id': record_id,
            'column': column,
            'new_value': new_value
        }
        request_queue.put(('update-' + str(time.time()), response_queue, request_data))
    try:
        result = response_queue.get(timeout=30)
        return jsonify(result), 200
    except queue.Empty:
        return jsonify({"error": "Request timed out"}), 504

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=50001, debug=True)
