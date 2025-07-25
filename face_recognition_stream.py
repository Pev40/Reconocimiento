import cv2
import face_recognition
import os
import numpy as np
from datetime import datetime
from pymongo import MongoClient

# Ruta a la carpeta de rostros conocidos
KNOWN_FACES_DIR = "known_faces"

# Cargar rostros conocidos
known_encodings = []
known_names = []

print("🔍 Cargando rostros conocidos...")
for filename in os.listdir(KNOWN_FACES_DIR):
    if filename.endswith(".jpg") or filename.endswith(".png"):
        image = face_recognition.load_image_file(f"{KNOWN_FACES_DIR}/{filename}")
        encoding = face_recognition.face_encodings(image)
        if encoding:
            known_encodings.append(encoding[0])
            known_names.append(os.path.splitext(filename)[0])

# RTMP stream
RTMP_URL = "rtmp://localhost/live/stream"
cap = cv2.VideoCapture(RTMP_URL)

# Conexión a MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["asistencia"]
collection = db["registros"]

print("📡 Procesando video...")

while True:
    ret, frame = cap.read()
    if not ret:
        print("⚠️ Esperando señal...")
        continue

    # Reducir tamaño para más velocidad
    small_frame = cv2.resize(frame, (0, 0), fx=0.25, fy=0.25)
    rgb_small = cv2.cvtColor(small_frame, cv2.COLOR_BGR2RGB)

    # Detección de rostros
    face_locations = face_recognition.face_locations(rgb_small)
    face_encodings = face_recognition.face_encodings(rgb_small, face_locations)

    for face_encoding, face_location in zip(face_encodings, face_locations):
        matches = face_recognition.compare_faces(known_encodings, face_encoding, tolerance=0.5)
        name = "Desconocido"

        face_distances = face_recognition.face_distance(known_encodings, face_encoding)
        best_match = np.argmin(face_distances)

        if matches[best_match]:
            name = known_names[best_match]

            # Guardar en MongoDB
            now = datetime.now()
            data = {
                "nombre": name,
                "fecha": now.strftime("%Y-%m-%d"),
                "hora": now.strftime("%H:%M:%S")
            }
            collection.insert_one(data)
            print(f"✅ {name} detectado a las {data['hora']}")

        # Dibujar rectángulo
        top, right, bottom, left = [v * 4 for v in face_location]  # volver al tamaño original
        cv2.rectangle(frame, (left, top), (right, bottom), (0, 255, 0), 2)
        cv2.putText(frame, name, (left, top - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0, 255, 0), 2)

    # Mostrar video (opcional)
    cv2.imshow("Reconocimiento Facial", frame)

    if cv2.waitKey(1) & 0xFF == ord("q"):
        break

cap.release()
cv2.destroyAllWindows()
