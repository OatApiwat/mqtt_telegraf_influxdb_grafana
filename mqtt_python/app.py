import paho.mqtt.client as mqtt
import json
import time
import random

MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/data"

client = mqtt.Client()
client.connect(MQTT_BROKER, MQTT_PORT, 60)
def generate_data():
    return {
        "data1": random.random(),
        "data2": random.random(),
        "data3": random.random(),
        "data4": random.random(),
        "data5": random.random(),
        "data6": random.random(),
        "data7": random.random(),
        "data8": random.random(),
        "data9": random.random(),
        "data10": random.random()
    }

while True:
    data = generate_data()
    client.publish(MQTT_TOPIC, json.dumps(data))
    print(f"Published: {data}")
    time.sleep(1)
