import random
import requests
from flask import Flask, request, jsonify

app = Flask(__name__)

SERVER_CONTAINERS = [
    'http://172.18.0.2:50001',
    'http://172.18.0.5:50001',
    'http://172.18.0.3:50001',
#    'http://172.18.0.6:50001',
]

def get_random_server():
    return random.choice(SERVER_CONTAINERS)

@app.route('/search/<column>', methods=['GET'])
def search(column):
    query_value = request.args.get('value')
    match_type = request.args.get('match_type', default='partial')
    if not query_value:
        return jsonify({"error": "Search value is required"}), 400

    server_url = get_random_server()
    print(f"Forwarding request to server: {server_url}")
    try:
        response = requests.get(f"{server_url}/search/{column}", params=request.args)
        return jsonify(response.json()), response.status_code
    except Exception as e:
        return jsonify({"error": f"Error forwarding request to server: {str(e)}"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=50001, debug=True)