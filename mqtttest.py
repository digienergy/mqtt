import paho.mqtt.client as mqtt

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

def on_disconnect(client, userdata, rc):
    print("Disconnected from MQTT broker")

# MQTT broker details
broker = "broker.hivemq.com"  # Replace with your broker address
port = 1883                  # Default MQTT port
topic = "data/device20250108"         # Topic to publish and subscribe to

# Create a new MQTT client instance
client = mqtt.Client()

# Assign the callback functions
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

try:
    # Connect to the MQTT broker
    client.connect(broker, port, keepalive=60)
    
    # Publish a message to the topic
    client.publish(topic, "Hello MQTT!")

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
