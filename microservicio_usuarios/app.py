from flask import Flask, request, jsonify, json
from werkzeug.security import generate_password_hash
import os, kafka,time
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)

producer = kafka.KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    api_version=(2, 0, 2)
)

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()

    username = data.get('usuario')
    email = data.get('email')
    password = data.get('contrasena')

    if not username or not email or not password:
        return jsonify({"error": "Faltan datos obligatorios"}), 400
    
    hashed_password = generate_password_hash(password)

    producer.send('usuarios', data)
    producer.flush()

    return jsonify({
        "mensaje": "Usuario recibido correctamente",
        "data": {
            "usuario": username,
            "email": email,
            "contrasena_encriptada": hashed_password
        }
    }), 201

if __name__ == '__main__':
    
    app.run(host='0.0.0.0', port=5000, debug=True)