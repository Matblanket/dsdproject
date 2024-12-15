import random
import requests
from flask import Flask, request, jsonify
from flask_cors import CORS
import threading

app = Flask(__name__)
CORS(app)

SERVER_CONTAINERS = [
    'http://172.18.0.2:50001',
    'http://172.18.0.3:50001',
]

server_connections = {server: 0 for server in SERVER_CONTAINERS}
lock = threading.Lock()

def get_least_connected_server():
    with lock:
        print("Current server connections:", server_connections)
        min_connections = min(server_connections.values())
        least_connected_servers = [server for server, count in server_connections.items() if count == min_connections]
        print("least connected servers",least_connected_servers)
        return random.choice(least_connected_servers)

def update_server_connections(server, increment=True):
    with lock:
        if increment:
            server_connections[server] += 1
        else:
            server_connections[server] -= 1

@app.route('/search/<column>', methods=['GET'])
def search(column):
    query_value = request.args.get('value')
    match_type = request.args.get('match_type', default='partial')
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400
    server_url = get_least_connected_server()
    print(f"Forwarding request to server: {server_url}")
    update_server_connections(server_url, increment=True)
    try:
        response = requests.get(f"{server_url}/search/{column}", params=request.args)
        update_server_connections(server_url, increment=False)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        update_server_connections(server_url, increment=False)
        return jsonify({"error": f"Error forwarding request to server: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=50001, debug=True)

