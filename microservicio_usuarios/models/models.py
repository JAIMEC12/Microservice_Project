#DEPENDENCIAS
from flask_sqlalchemy import SQLAlchemy #EXTENSION QUE PERMITE INTEGRAR SQLITE CON FLASK

db = SQLAlchemy() #CREA UNA INSTANCIA GLOBAL DE SQLALCHEMY

#CLASE QUE REPRESENTA LA TABLA DE USUARIOS
class Usuario(db.Model):
    __tablename__ = 'usuarios' #NOMBRE DE LA TABLA
    #CAMPOS DE LA TABLA
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    usuario = db.Column(db.String(100), nullable=False, unique=True)
    email = db.Column(db.String(100), nullable=False, unique=True)
    contrasena = db.Column(db.String(100), nullable=False)
    #PARA DEBUGUEAR Y VER COMO SE VA A MOSTRAR EL OBJETO CUANDO DEBE IMPRIMIRSE EN CONSOLA
    def __repr__(self):
        return f'<Usuario {self.usuario}>'
