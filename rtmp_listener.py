# rtmp_listener.py
import cv2
from kafka import KafkaProducer
import numpy as np
import json

# Configuración de Kafka
KAFKA_BROKER = "54.146.92.176:9092,54.82.61.71:9092,3.88.98.112:9092" # Cambia por la IP/DNS del servidor Kafka

TOPIC = "raw_frames"

# Inicializar productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=[
        "54.146.92.176:9092",
        "54.82.61.71:9092",
        "3.88.98.112:9092"
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


# Dirección RTMP
RTMP_URL = "rtmp://127.0.0.1/live/stream"  # si el stream corre en la misma máquina

# OpenCV para abrir el stream RTMP
cap = cv2.VideoCapture(RTMP_URL)

if not cap.isOpened():
    print("❌ No se pudo abrir el stream RTMP.")
    exit()

print("📡 Recibiendo video desde RTMP...")

while True:
    ret, frame = cap.read()
    if not ret:
        print("⚠️ No se pudo leer el frame. Esperando...")
        continue

    # Convertir frame a un formato serializable (por ejemplo, JPEG)
    _, buffer = cv2.imencode(".jpg", frame)
    frame_data = buffer.tobytes()

    # Enviar frame a Kafka
    producer.send(TOPIC, {"frame": frame_data.hex(), "timestamp": str(np.datetime64("now"))})
    producer.flush()

    # Mostrar video (opcional, para depuración)
    cv2.imshow("Stream RTMP", frame)

    if cv2.waitKey(1) & 0xFF == ord("q"):
        break

cap.release()
cv2.destroyAllWindows()
producer.close()