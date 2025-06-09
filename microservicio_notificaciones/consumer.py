from flask import Flask, jsonify
from kafka import KafkaConsumer
import json, threading

app = Flask(__name__)

# Lista para almacenar notificaciones (en producción usa una base de datos)
notifications = []

# Configurar Kafka Consumer
consumer = KafkaConsumer(
    'usuarios',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='notification_group',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    api_version=(2, 0, 2)
)

def kafka_consumer_thread():
    #Hilo para escuchar mensajes de Kafka"""
    for message in consumer:
        try:
            notification_data = message.value
            notifications.append(notification_data)
            print(f"Nueva notificación recibida: {notification_data}")
            
        except Exception as e:
            print(f"Error procesando mensaje: {e}")

@app.route('/notifications', methods=['GET'])
def get_notifications():
    #Endpoint para ver todas las notificaciones"""
    return jsonify({
        "total": len(notifications),
        "notifications": notifications
    })

@app.route('/notifications/latest', methods=['GET'])
def get_latest_notification():
    #Endpoint para ver la última notificación"""
    if notifications:
        return jsonify(notifications[-1])
    return jsonify({"message": "No notifications yet"}), 404

# @app.route('/health', methods=['GET'])
# def health_check():
#     return jsonify({"status": "Notification Service is running"}), 200

if __name__ == '__main__':
    # Iniciar el consumidor de Kafka en un hilo separado
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start()
    
    app.run(host='0.0.0.0',port=5001, debug=True)