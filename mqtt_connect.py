import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import json
from dotenv import load_dotenv
import os

load_dotenv()
MQTT_BROKER=os.getenv("MQTT_BROKER")
KAFKA_BROKER=os.getenv("KAFKA_BROKER")
# Define the callback functions
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected successfully!")
        # Subscribe to a topic once connected
        client.subscribe("data/device20250108")
    else:
        print(f"Connection failed with code {rc}")

def on_message(client, userdata, msg):
    print(f"Received message '{msg.payload.decode()}' on topic '{msg.topic}'")
    
    payload = msg.payload.decode()
    data = json.loads(payload)

    result={}
    for item in data.get("d", []):
        tag = item["tag"].split(":")[-1]  # 提取 ":" 后面的内容
        str_value=str(int(item["value"]))
        value = int(str_value,16)
        result[tag]=value
    
    # 添加时间戳
    timestamp = data["ts"]
    result["time"]=timestamp
    message_str = json.dumps(result)
    result = message_str.encode('utf-8')

    #Send the received MQTT message to Kafka
    send_to_kafka(result)

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker")

# Kafka configuration
KAFKA_BROKER = KAFKA_BROKER  # Replace with your Kafka broker address
KAFKA_TOPIC = 'mqtt_data_topic'  # Kafka topic to send messages to

# Initialize Kafka Producer
producer = Producer({'bootstrap.servers': KAFKA_BROKER})

# Function to send message to Kafka
def send_to_kafka(message):
    try:
        # Send the message to Kafka topic
        producer.produce(KAFKA_TOPIC, value=message)
        producer.flush()  # Ensure message is sent immediately
        print(f"Sent message to Kafka: {message}")
    except Exception as e:
        print(f"Failed to send message to Kafka: {e}")

# MQTT broker details
broker = MQTT_BROKER  # Replace with your broker address
port = 1883                  # Default MQTT port
topic = "data/device20250108"  # Topic to publish and subscribe to

# Create a new MQTT client instance
client = mqtt.Client()

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

try:
    # Connect to the MQTT broker
    client.connect(broker, port, keepalive=60)

    # Start the network loop
    client.loop_start()

    # Keep the script running to receive messages
    input("Press Enter to exit...\n")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Disconnect the client
    client.loop_stop()
    client.disconnect()
