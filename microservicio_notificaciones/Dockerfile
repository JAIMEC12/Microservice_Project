FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install flask kafka-python 
CMD ["python", "consumer.py"]