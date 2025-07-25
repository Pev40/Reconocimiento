import cv2
from kafka import KafkaProducer
import numpy as np
import json
import time
import os

# Configuración de Kafka
KAFKA_BROKER = "54.146.92.176:9092,54.82.61.71:9092,3.88.98.112:9092"  # IPs elásticas de tus instancias Kafka
TOPIC = "raw_frames"

# Inicializar productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=[
        "54.146.92.176:9092",
        "54.82.61.71:9092",
        "3.88.98.112:9092"
    ],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    compression_type="gzip"  # Opcional: Comprime datos para eficiencia
)

# Dirección RTMP (ajusta según tu configuración)
RTMP_URL = "rtmp://127.0.0.1/live/stream2"  # Cambia si el stream no corre localmente
# Si el stream viene de otra máquina (por ejemplo, FFmpeg en otra EC2), usa la IP pública:
# RTMP_URL = "rtmp://<ip-pública-fase-c>/live/stream"

# OpenCV para abrir el stream RTMP en modo headless
cap = cv2.VideoCapture(RTMP_URL, cv2.CAP_FFMPEG)  # Especifica CAP_FFMPEG para mejor compatibilidad con RTMP

if not cap.isOpened():
    print("❌ No se pudo abrir el stream RTMP. Verifica la URL y el puerto 1935.")
    exit(1)

print("📡 Recibiendo video desde RTMP...")

frame_count = 0
try:
    while True:
        ret, frame = cap.read()
        if not ret:
            print("⚠️ No se pudo leer el frame. Reintentando en 1 segundo...")
            time.sleep(1)
            continue

        frame_count += 1
        # Convertir frame a un formato serializable (JPEG)
        _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 80])  # Calidad 80% para reducir tamaño
        frame_data = buffer.tobytes()

        # Crear mensaje con metadatos
        message = {
            "frame": frame_data.hex(),  # Enviar datos binarios como string hexadecimal
            "timestamp": str(np.datetime64("now")),
            "frame_id": f"frame_{frame_count}_{int(time.time())}"
        }

        # Enviar frame a Kafka
        producer.send(TOPIC, message)
        producer.flush()  # Asegura que los datos se envíen inmediatamente

        # Log básico (puedes redirigir a un archivo si lo deseas)
        print(f"Frame {frame_count} procesado y enviado a {TOPIC} a las {time.strftime('%H:%M:%S')}")

except KeyboardInterrupt:
    print("⏹️ Deteniendo el listener por interrupción del usuario...")
except Exception as e:
    print(f"❌ Error crítico: {e}")
finally:
    cap.release()
    producer.close()
    print("🔚 Listener detenido.")