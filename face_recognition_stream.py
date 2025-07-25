import cv2
import numpy as np
from kafka import KafkaProducer, KafkaConsumer
from hdfs import InsecureClient
from pymongo import MongoClient
import face_recognition
import face_recognition_models

face_recognition.api.pose_predictor_model_location = "/home/ubuntu/.face_recognition_models/shape_predictor_68_face_landmarks.dat"
face_recognition.api.face_recognition_model_location = "/home/ubuntu/.face_recognition_models/dlib_face_recognition_resnet_model_v1.dat"
face_recognition.api.cnn_face_detector_model_location = "/home/ubuntu/.face_recognition_models/mmod_human_face_detector.dat"

print("✅ Los modelos se han cargado correctamente.")

import json
import time
from datetime import datetime

# Configuración de Kafka
KAFKA_BROKER = "54.146.92.176:9092,54.82.61.71:9092,3.88.98.112:9092"
INPUT_TOPIC = "raw_frames"  # Recibe frames desde rtmp_listener.py
OUTPUT_TOPIC = "eventos"    # Envía eventos a MongoDB

# Configuración de HDFS
HDFS_URL = "http://172.31.18.31:9870"  # Reemplaza con la IP privada del NameNode
HDFS_USER = "ubuntu"
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# Configuración de MongoDB Atlas
MONGO_URI = "mongodb+srv://pvizcarra:<db_password>@cluster0.y0xt7dp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Reemplaza <db_password> con la contraseña real del usuario pvizcarra en MongoDB Atlas
MONGO_URI = MONGO_URI.replace("<db_password>", "11eHEjDtKfWE6sNs")  # Cambia esto
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["eventos_db"]
collection = db["eventos"]

# Configuración del productor y consumidor de Kafka
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
consumer = KafkaConsumer(INPUT_TOPIC, bootstrap_servers=KAFKA_BROKER, auto_offset_reset='earliest', group_id='face_recognition_group')

# Función para guardar frames en HDFS
def save_to_hdfs(image, filename, path):
    try:
        _, buffer = cv2.imencode('.jpg', image)
        hdfs_path = f"{path}/{filename}"
        hdfs_client.write(hdfs_path, buffer.tobytes(), overwrite=True)
        print(f"Guardado {filename} en {path}")
    except Exception as e:
        print(f"Error al guardar en HDFS: {e}")

# Función para guardar logs en HDFS
def log_to_hdfs(message, filename):
    try:
        hdfs_path = f"/data/logs/{filename}"
        hdfs_client.write(hdfs_path, message.encode('utf-8'), overwrite=True)
        print(f"Log guardado: {filename}")
    except Exception as e:
        print(f"Error al guardar log en HDFS: {e}")

# Procesamiento de frames y detección de rostros
def process_frame(frame_data):
    try:
        # Depuración: Verificar el contenido recibido
        print(f"Tamaño de frame_data: {len(frame_data)} bytes")
        if not frame_data or len(frame_data) == 0:
            print("Frame no decodificado: datos vacíos o corruptos")
            return

        # Intentar decodificar como JPEG
        nparr = np.frombuffer(frame_data, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if frame is None:
            # Depuración adicional: Guardar los bytes recibidos para análisis
            with open("/tmp/corrupted_frame.bin", "wb") as f:
                f.write(frame_data)
            print("Frame no decodificado: posible corrupción. Guardado en /tmp/corrupted_frame.bin para análisis")
            return

        # Detectar rostros
        rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        face_locations = face_recognition.face_locations(rgb_frame)
        face_encodings = face_recognition.face_encodings(rgb_frame, face_locations)

        # Generar nombre de archivo único
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        frame_filename = f"frame_{timestamp}.jpg"
        image_filename = f"image_{timestamp}.jpg"

        # Guardar el frame original en HDFS
        save_to_hdfs(frame, frame_filename, "/data/frames")

        # Procesar y guardar imagen con rostros detectados
        if face_locations:
            for (top, right, bottom, left) in face_locations:
                cv2.rectangle(frame, (left, top), (right, bottom), (0, 0, 255), 2)
            save_to_hdfs(frame, image_filename, "/data/images")

            # Crear evento JSON
            event = {
                "timestamp": timestamp,
                "frame_id": frame_filename,
                "image_id": image_filename,
                "face_count": len(face_locations),
                "status": "face_detected"
            }
            event_json = json.dumps(event).encode('utf-8')

            # Enviar evento a Kafka
            producer.send(OUTPUT_TOPIC, event_json)

            # Guardar evento en MongoDB Atlas
            collection.insert_one(event)
            print(f"Evento guardado en MongoDB y enviado a Kafka: {event}")

        # Guardar log
        log_message = f"Procesado frame {frame_filename} a las {timestamp} con {len(face_locations)} rostros detectados\n"
        log_to_hdfs(log_message, f"log_{timestamp}.txt")

    except Exception as e:
        print(f"Error procesando frame: {e}")

# Bucle principal
if __name__ == "__main__":
    print("✅ Los modelos se han cargado correctamente.")
    print("Iniciando consumidor de Kafka para detección de rostros...")
    for message in consumer:
        try:
            # Decodificar el mensaje JSON
            msg = json.loads(message.value.decode("utf-8"))

            # Convertir el string hexadecimal de vuelta a bytes
            frame_data = bytes.fromhex(msg["frame"])

            # Procesar el frame
            process_frame(frame_data)

        except Exception as e:
            print(f"Error en el consumidor: {e}")
        time.sleep(0.1)

    producer.close()
    mongo_client.close()