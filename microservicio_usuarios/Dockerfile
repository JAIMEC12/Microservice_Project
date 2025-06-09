FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install flask kafka-python flask_sqlalchemy
CMD ["python", "app.py"]