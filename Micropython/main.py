from machine import Pin
from umqtt.simple import MQTTClient
import ujson
import network
import utime as time
import dht

# Device Setup
DEVICE_ID = "wokwi001"

# Wifi Setup
WIFI_SSID = "Wokwi-GUEST"
WIFI_PASSWORD = ""

# MQTT Setup
MQTT_BROKER = "mqtt-dashboard.com" #Check on the HiveMQ
MQTT_CLIENT = DEVICE_ID
MQTT_TELEMETRY_TOPIC = "IoT/Telemetry"
MQTT_CONTROL_TOPIC = "IoT/Control"

# DHT Sensor Setup
DHT_PIN = Pin(15)

# LED Setup
RED_LED = Pin(12, Pin.OUT)
BLUE_LED = Pin(13, Pin.OUT)
RED_LED.on()
BLUE_LED.on()

# Methods
def did_receive_callback(topic, message):
  print('Data received!\n')
  print('Topic = {0}, Message = {1}'.format(topic, message))

def mqtt_connect():
  print('Connecting to MQTT Broker...\n', end="")
  mqtt_client = MQTTClient(MQTT_CLIENT, MQTT_BROKER)
  mqtt_client.set_callback(did_receive_callback)
  mqtt_client.connect()
  print('Connected!')
  mqtt_client.subscribe(MQTT_CONTROL_TOPIC)
  return mqtt_client 

def create_json_data(temperature, humidity):
  data = ujson.dumps({
    "temp": temperature,
    "humidity": humidity,
  })
  return data

def mqtt_client_publish(topic, data):
  print(f'Publishing to {topic}: {data}')
  try:
    mqtt_client.publish(topic, data)
    print('Published successfully!')
  except Exception as e:
    print(f'Publish failed: {e}')
  print('\nUpdating MQTT Broker...')
  print(data)

# Connect to Wifi
wifi_client = network.WLAN(network.STA_IF)
wifi_client.active(True)
print('Connecting device to Wifi')
wifi_client.connect(WIFI_SSID, WIFI_PASSWORD)

# Wait until Wifi is Connected
while not wifi_client.isconnected():
  print('Connecting')
  time.sleep(0.1)
print('Wifi connected!')
print(wifi_client.ifconfig())

# Connect to MQTT
mqtt_client = mqtt_connect()
RED_LED.off()
BLUE_LED.off()
dht_sensor = dht.DHT22(DHT_PIN)
telemetry_data_old = ""

while True:
  mqtt_client.check_msg()
  print('. ', end="")
  dht_sensor.measure()
  telemetry_data_new = create_json_data(dht_sensor.temperature(), dht_sensor.humidity())
  
  if telemetry_data_new != telemetry_data_old:
    mqtt_client_publish(MQTT_TELEMETRY_TOPIC, telemetry_data_new)
    telemetry_data_old = telemetry_data_new
  
  time.sleep(0.1)