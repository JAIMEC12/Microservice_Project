#DEPENDENCIAS 
from flask import Flask, jsonify, json #FUNCIONES DEL FRAMEWORK WEB PARA EL MANEJO DE SOLICITUDES DE DATOS
from kafka import KafkaConsumer #COMUNICACION CON EL KAFKA, EN ESTE CASO, EL CONSUMIDOR DE EVENTOS 
import threading #FUNCION PARA EL MANEJO Y CREACION DE HILOS DE PROCESOS

#INSTANCIA DE LA APLICACION FLASK
app = Flask(__name__)


# ARREGLO DONDE SE VA IR ALMACENANDO LAS NOTIFICACIONES 
notifications = []


# CONFIGURACION DEL CONSUMER DEL KAFKA
consumer = KafkaConsumer(
    'usuarios', #TOPIC DEL KAFKA DEL CUAL DEBE LEER LOS MENSAJES
    bootstrap_servers=['kafka:9092'], #DIRECCION DEL SERVIDOR DE KAFKA
    auto_offset_reset='latest', #EMPIEZA LEYENDO DESDE LOS MENSAJES MAS RECIENTES
    enable_auto_commit=True, #CONFIRMA QUE LOS MENSAJES FUERON PROCESADOS
    group_id='notification_group', #IDENTIFICA ESTE CONSUMIDOR COMO PARTE POR NOTIFICATION_GROUP
    value_deserializer=lambda m: json.loads(m.decode('utf-8')), #CONVIERTE LOS BYTES EN OBJETOS PYTHON
    api_version=(2, 0, 2) #API DEL KAFKA
)


#ESTO ES UN HILO EL CUAL SE EJECUTA CONCURRENTEMENTE CON LA INSTANCIA DE FLASK
def kafka_consumer_thread():
    #CICLO INFINITO ESCUCHANDO MENSAJES DE KAFKA
    for message in consumer:
        try:
            #POR CADA MENSAJE PROCESADO CORRECTAMENTE LO AGREGA AL ARREGLO DE NOTIFICACIONES
            notification_data = message.value
            notifications.append(notification_data)
            print(f"Nueva notificación recibida: {notification_data}")
            
        except Exception as e:
            #PARA EVITAR QUE SE CAIGA EL SISTEMA, SE MANEJA LOS ERRORES
            print(f"Error procesando mensaje: {e}")


#DEFINICION DEL ENPOINT PARA LAS NOTIFICACIONES
@app.route('/notifications', methods=['GET'])
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