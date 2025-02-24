import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
import requests
import json

# MongoDB Atlas Configuration
MONGO_URI = "mongodb+srv://ShijinForge:<JasJerFarVal>@shijinforge2.eoopz.mongodb.net/?retryWrites=true&w=majority&appName=ShijinForge2"
mongo_client = MongoClient(MONGO_URI)
db = mongo_client["project_2"]  # Database name
collection = db["sensor_data"]  # Collection name

# Ubidots Configuration
TOKEN = "BBUS-2CnMsYYSV9hgXRjc6lTgCKZYi9dECy"
DEVICE_LABEL = "ESP32"
UBIDOTS_URL = f"https://industrial.api.ubidots.com/api/v1.6/devices/{DEVICE_LABEL}"
HEADERS = {"X-Auth-Token": TOKEN, "Content-Type": "application/json"}

# MQTT Broker Configuration
MQTT_BROKER = "broker.hivemq.com"
MQTT_PORT = 8884
MQTT_TOPIC = "IoT/Telemetry"

# \MongoDB Atlas
def store_in_mongodb(data):
    try:
        data["timestamp"] = datetime.utcnow().isoformat()
        collection.insert_one(data)
        print("[INFO] Data stored in MongoDB")
    except Exception as e:
        print(f"[ERROR] Failed to store data in MongoDB: {e}")

# Ubidots
def send_to_ubidots(data):
    try:
        response = requests.post(UBIDOTS_URL, headers=HEADERS, json=data)
        if response.status_code == 200:
            print("[INFO] Data sent to Ubidots successfully")
        else:
            print(f"[ERROR] Failed to send data to Ubidots: {response.status_code}, {response.text}")
    except Exception as e:
        print(f"[ERROR] Ubidots request failed: {e}")


def process_sensor_data(data):
    store_in_mongodb(data)  # Save to MongoDB
    send_to_ubidots(data)   # Send to Ubidots


# MQTT Callbacks
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[INFO] Connected to MQTT Broker")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[ERROR] Failed to connect, return code {rc}")

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print(f"[INFO] Received data: {payload}")
        process_sensor_data(payload)
    except Exception as e:
        print(f"[ERROR] Failed to process message: {e}")


# Initialize n start MQTT Client
mqtt_client = mqtt.Client()
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.connect(MQTT_BROKER, MQTT_PORT)

print("[INFO] Listening for MQTT messages...")
mqtt_client.loop_forever()  # Keep running
