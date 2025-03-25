import paho.mqtt.client as mqtt
import json
import time
import random

MQTT_BROKER = "mosquitto"
MQTT_PORT = 1883
MQTT_TOPIC = "iot/data_2"
count = 0
def generate_data():
    global count
    count = count+1
    return {
        "data1": count,
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

def main():
    while True:
        data = generate_data()
        client.publish(MQTT_TOPIC, json.dumps(data))
        print(f"Published: {data}")
        time.sleep(1)

if __name__ == "__main__":
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    main()
