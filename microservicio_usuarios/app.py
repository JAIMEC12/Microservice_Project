#DEPENDENCIAS 
from flask import Flask, request, jsonify, json #FUNCIONES DEL FRAMEWORK WEB PARA EL MANEJO DE SOLICITUDES DE DATOS
from werkzeug.security import generate_password_hash #ENCRIPTACION DE CONTRASENIA
from models.models import db, Usuario  #MODELO Y CONFIGURACION DE LA BASE DE DATOS
from kafka import KafkaProducer #PARA MANEJAR Y GESTIONAR APACHE KAFKA ENTRE LOS DOS MICROSERVICIO, EN ESTE CASO, EL QUE PUBLICA LOS EVENTOS

#CREA UNA INSTANCIA, O COMO TAL LA APLICACION WEB PRINCIPAL
app = Flask(__name__)

# CONFIGURACION DE LA BASE DE DATOS, SQLITE POR SU SIMPLEZA Y PORQUE ES SERVERLESS
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///usuarios.db' #CONFIGURA LA URL, O UBICACION DONDE VA A ESTAR EL ARCHIVO DE LA BASE DE DATOS
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False #DESACTIVA EL SEGUIMIENTO DE MODIFICACIONES DE SQLALCHEMY
#INICIALIZA LA BASE DE DATOS CON LA APLICACION FLASK
db.init_app(app)

# INICIALIZA Y CONFIGURA LA COMUNICACION DEL KAFKA, EN ESTE CASO, CONSTRUYENDO EL PRODUCER (PUBLICADOR) DE KAFKA, EVENTOS.
producer = KafkaProducer(
   bootstrap_servers=['kafka:9092'], #DIRECCION DEL SERVIDOR DE KAFKA
   value_serializer=lambda v: json.dumps(v).encode('utf-8'), #CONVIERTE OBJETOS PYTHON A JSON LUEGO A BYTES
   api_version=(2, 0, 2) #ESPECIFICA LA VERSION DE API DEL KAFKA
)


#DEFINICION DEL ENPOINT PARA LA API REST, CON EL METODO POST, PARA EL ENVIO DE DATOS, REGISTRO DE USUARIOS EN ESTE CASO
@app.route('/register', methods=['POST']) #DEFINE LA RUTE Y EL METODO
def register():
    data = request.get_json() #SE EXTRAE LOS DATOS DADOS POR LA PETICION HTTP(POST)

    #EXTRAE LOS CAMPOS ESPECIFICOS DEL JSON RECIBIDO Y ALMACENADO EN DATA
    username = data.get('usuario')
    email = data.get('email')
    password = data.get('contrasena')

    #VALIDACION SI TODOS LOS CAMPOS FUERON LLENADOS
    if not username or not email or not password:
        return jsonify({"error": "Faltan datos obligatorios"}), 400 #DE LO CONTRARIO ENTREGA UN JSON INDICANDO ERROR EN CONJUNTO CON EL ESTADO DE LA PETICION
    
    #ENCRIPTA LA CONTRASENIA ANTES DE SER ALMACENADA EN LA BASE DE DATOS
    hashed_password = generate_password_hash(password)

    #CREA UNA VARIABLE LA CUAL REPRESENTA EL REGISTRO DENTRO DE LA TABLA USUARIOS
    nuevo_usuario = Usuario(usuario=username, email=email, contrasena=hashed_password)
    db.session.add(nuevo_usuario) #LO ANADE A LA SESION DE LA BASE DE DATOS
    db.session.commit() #CONFIRMA LA TRANSACCION

    # LUEGO ENVIA EL JSON ANTERIORMENTE EXTRAIDO AL OTRO MICROSERVICIO A TRAVES DE KAFKA
    producer.send('usuarios', data) #ENVIA LOS DATOS DEL USUARIO AL TOPIC USUARIOS DE KAFKA
    producer.flush() #ASEGURA QUE EL MENSAJE SEA ENVIADO INMEDIATAMENTE
    return jsonify({
        "mensaje":f"El usuario {username} ha sido creado",
        "status": "Registrado"
    }), 200


if __name__ == '__main__':
    with app.app_context():
        db.create_all() #CREA LA BASE DE DATOS CON SUS TABLAS RESPECTIVAS, EN CASO DE QUE NO EXISTAN
    app.run(host='0.0.0.0', port=5000, debug=True) #EJECUTA LA APLICACION FLASK EN EL PUERTO 5000