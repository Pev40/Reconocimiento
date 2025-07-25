# rtmp_listener.py
import cv2

# Dirección RTMP enviada desde el cliente (PC con FFmpeg)
# Asegúrate de cambiarlo si usas otro puerto o ruta
RTMP_URL = "rtmp://0.0.0.0/live/stream"

# OpenCV puede abrir directamente una fuente RTMP
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

    # Procesamiento o visualización
    cv2.imshow("Stream RTMP", frame)

    if cv2.waitKey(1) & 0xFF == ord("q"):
        break

cap.release()
cv2.destroyAllWindows()
