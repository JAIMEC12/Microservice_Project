#DEPENDENCIAS 
from flask import Flask, jsonify, json #FUNCIONES DEL FRAMEWORK WEB PARA EL MANEJO DE SOLICITUDES DE DATOS
from kafka import KafkaConsumer #COMUNICACION CON EL KAFKA, EN ESTE CASO, EL CONSUMIDOR DE EVENTOS 
import threading, time #FUNCION PARA EL MANEJO Y CREACION DE HILOS DE PROCESOS
from kafka.errors import NoBrokersAvailable
#INSTANCIA DE LA APLICACION FLASK
app = Flask(__name__)


# ARREGLO DONDE SE VA IR ALMACENANDO LAS NOTIFICACIONES 
notifications = []

def kafka_consumer_thread():
    consumer = None
    while True:
        try:
            if consumer is None:
                print("🔄 Intentando conectar a Kafka...")
                consumer = KafkaConsumer(
                    'usuarios',
                    bootstrap_servers=['kafka:9092'],
                    auto_offset_reset='earliest',
                    enable_auto_commit=True,
                    client_id = "notificaciones",
                    group_id='notification_group',
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    api_version=(2, 0, 2),
                    # consumer_timeout_ms=1000,
                    # reconnect_backoff_max_ms = 3000,
                    # # CONFIGURACIONES MÁS ESTABLES PARA EVITAR REBALANCING
                    # session_timeout_ms = 6000,       # 60 segundos (más tiempo)
                    # # heartbeat_interval_ms = 2000,    # 20 segundos (más tiempo)  
                    # # max_poll_interval_ms = 6000,    # 10 minutos (más tiempo)
                    # request_timeout_ms = 7000,       # 60 segundos
                    # retry_backoff_ms = 2000,          # 2 segundos entre reintentos
                    # reconnect_backoff_ms = 2000,      # 2 segundos para reconexión
                    # # max_poll_records = 10,            # Menos registros por poll (más estable)
                    # # fetch_min_bytes = 1,              
                    # # fetch_max_wait_ms = 1000,         # 1 segundo de espera
                    #  connections_max_idle_ms = 7400 # 9 minutos antes de cerrar conexión idle
                    # 
                    )
                print("✅ Conectado a Kafka exitosamente")
            
            for message in consumer:
                notification_data = message.value
                notifications.append(notification_data)
                print(f"Nueva notificación recibida: {notification_data}")
                
        except NoBrokersAvailable:
            print("⚠️  Kafka no disponible, reintentando en 5 segundos...")
            if consumer:
                consumer.close()
                consumer = None
            time.sleep(5)
        except Exception as e:
            print(f"❌ Error en consumer: {e}")
            if consumer:
                consumer.close()
                consumer = None
            time.sleep(5)


#DEFINICION DEL ENPOINT PARA LAS NOTIFICACIONES
@app.route('/notificationes', methods=['GET'])
def get_notifications():
    #RETORNA UN JSON CON EL TOTAL DE NOTIFICACIONES Y TODAS LAS NOTIFICACIONES QUE SE HA CONSUMIDO
    return jsonify({
        "mesanje":"Los siguientes usuarios fueron registrados:",
        "total": len(notifications),
        "notificaciones": notifications
    }), 200


if __name__ == '__main__':
    # CREA UN HILO E LO INICIALIZA EJECUTANDO EL CONSUMIDOR DE KAFKA EN SEGUNDO PLANO
    kafka_thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    kafka_thread.start() 
    #INICIA LA INSTANCIA DE LA APLICACION FLASK EN EL PUERTO 5001
    app.run(host='0.0.0.0',port=5001, debug=True)