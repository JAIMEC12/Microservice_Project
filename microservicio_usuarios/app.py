from flask import Flask, request, jsonify
from werkzeug.security import generate_password_hash
import os

app = Flask(__name__)

@app.route('/register', methods=['POST'])
def register():
    data = request.get_json()

    username = data.get('usuario')
    email = data.get('email')
    password = data.get('contrasena')

    if not username or not email or not password:
        return jsonify({"error": "Faltan datos obligatorios"}), 400
    
    hashed_password = generate_password_hash(password)

    return jsonify({
        "mensaje": "Usuario recibido correctamente",
        "data":{
            "usuario": username,
            "email": email,
            "contrasena_encriptada": hashed_password
        }
    }), 201

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)