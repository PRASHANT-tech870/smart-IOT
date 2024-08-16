import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials, db, firestore

# Firebase setup
cred = credentials.Certificate('/serviceAccountKey.json')

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://trail-befa6-default-rtdb.asia-southeast1.firebasedatabase.app/'
    })

# Initialize Firestore
firestore_db = firestore.client()

# MQTT setup
mqtt_broker = "localhost"  # Replace with your MQTT broker address
mqtt_port = 1883
mqtt_topic = "sensor/dht11"  # Updated topic

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe(mqtt_topic)

def on_message(client, userdata, msg):
    print(f"Topic: {msg.topic}\nMessage: {msg.payload.decode()}")
    data = msg.payload.decode()

    try:
        # Extract sensor values
        parts = data.split(' ')  # Split by space to parse each part
        temperature = float(parts[1])  # Extract temperature value
        humidity = float(parts[3])     # Extract humidity value
        ldr_value = int(parts[6])      # Extract LDR value

    except (IndexError, ValueError) as e:
        print("Error parsing message:", e)
        return

    # Prepare data for Firebase Realtime Database
    realtime_data = {
        "Temperature": temperature,
        "Humidity": humidity,
        "LDR": ldr_value
    }

    # Debugging output
    print("Sending data to Firebase Realtime Database:", realtime_data)

    try:
        # Send data to Firebase Realtime Database
        db.reference('/CombinedSensorData').update(realtime_data)  # Use update to avoid overwriting
        print("Data sent to Firebase Realtime Database")
    except Exception as e:
        print("Error sending data to Firebase Realtime Database:", e)

    # Prepare data for Firestore
    firestore_data = {
        "Temperature": temperature,
        "Humidity": humidity,
        "LDR": ldr_value
    }

    # Debugging output
    print("Sending data to Firestore:", firestore_data)

    try:
        # Send data to Firestore
        firestore_db.collection('combined_sensor_data').add(firestore_data)
        print("Data sent to Firebase Firestore")
    except Exception as e:
        print("Error sending data to Firebase Firestore:", e)

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect(mqtt_broker, mqtt_port, 60)
client.loop_forever()
