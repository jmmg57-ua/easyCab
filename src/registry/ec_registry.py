from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash, check_password_hash
import json
import os

app = Flask(__name__)
DATA_FILE = "taxis.json"

# Cargar base de datos
def load_data():
    if not os.path.exists(DATA_FILE):
        return {}
    with open(DATA_FILE, "r") as file:
        return json.load(file)

# Guardar base de datos
def save_data(data):
    with open(DATA_FILE, "w") as file:
        json.dump(data, file, indent=4)

@app.route('/register', methods=['POST'])
def register_taxi():
    data = request.json
    taxi_id = data.get("taxi_id")
    if not taxi_id:
        return jsonify({"error": "Taxi ID is required"}), 400

    taxis = load_data()
    if taxi_id in taxis:
        return jsonify({"error": "Taxi already registered"}), 409

    taxis[taxi_id] = {
        "status": "active",
        "registered_at": request.remote_addr
    }
    save_data(taxis)
    return jsonify({"message": "Taxi registered successfully"}), 201

@app.route('/deregister/<taxi_id>', methods=['DELETE'])
def deregister_taxi(taxi_id):
    taxis = load_data()
    if taxi_id not in taxis:
        return jsonify({"error": "Taxi not found"}), 404

    del taxis[taxi_id]
    save_data(taxis)
    return jsonify({"message": "Taxi deregistered successfully"}), 200

@app.route('/status/<taxi_id>', methods=['GET'])
def taxi_status(taxi_id):
    taxis = load_data()
    if taxi_id not in taxis:
        return jsonify({"error": "Taxi not found"}), 404

    return jsonify({"status": taxis[taxi_id]["status"]}), 200

if __name__ == '__main__':
    # Usar certificados SSL para HTTPS
    app.run(ssl_context=('cert.pem', 'key.pem'), debug=True)
