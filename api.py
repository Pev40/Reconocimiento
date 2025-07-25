from fastapi import FastAPI, HTTPException, WebSocket
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from hdfs import InsecureClient
import json
from datetime import datetime
from fastapi.responses import JSONResponse

# Inicializar FastAPI
app = FastAPI(title="Real-Time Assistance API")

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Ajusta en producción
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuración de MongoDB Atlas
MONGO_URI = "mongodb+srv://pvizcarra:<db_password>@cluster0.y0xt7dp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
# Reemplaza <db_password> con la contraseña real del usuario pvizcarra en MongoDB Atlas
MONGO_URI = MONGO_URI.replace("<db_password>", "11eHEjDtKfWE6sNs")  # Cambia esto
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["eventos_db"]
collection = db["eventos"]

# Configuración de HDFS
HDFS_URL = "http://172.31.18.31:9870"  # Reemplaza con la IP privada del NameNode
HDFS_USER = "ubuntu"
hdfs_client = InsecureClient(HDFS_URL, user=HDFS_USER)

# WebSocket para eventos en tiempo real
@app.websocket("/ws/events")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Obtener los últimos 10 eventos
            events = list(collection.find().sort("timestamp", -1).limit(10))
            for event in events:
                event["_id"] = str(event["_id"])
            await websocket.send_json({"status": "success", "events": events})
            await websocket.send_json({"status": "heartbeat", "time": datetime.now().isoformat()})
            await asyncio.sleep(1)  # Enviar cada segundo
    except Exception as e:
        await websocket.send_json({"status": "error", "message": str(e)})
    finally:
        await websocket.close()

# Endpoint para obtener un frame o imagen desde HDFS
@app.get("/media/{file_id}")
async def get_media(file_id: str):
    try:
        image_path = f"/data/images/{file_id}"
        if hdfs_client.status(image_path, strict=False):
            image_data = hdfs_client.read(image_path)
            return {"status": "success", "file_id": file_id, "data": image_data.hex(), "type": "image/jpeg"}

        frame_path = f"/data/frames/{file_id}"
        if hdfs_client.status(frame_path, strict=False):
            frame_data = hdfs_client.read(frame_path)
            return {"status": "success", "file_id": file_id, "data": frame_data.hex(), "type": "image/jpeg"}

        raise HTTPException(status_code=404, detail="File not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching media: {str(e)}")

# Endpoint de salud
@app.get("/health")
async def health_check():
    return {"status": "healthy", "time": datetime.now().isoformat()}

import asyncio