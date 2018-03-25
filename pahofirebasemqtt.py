'''
This code transmits the humidity, wind speed, 
and temperature data given from MQTT
published message to the Firebase Realtime Database 
using the Firebase SDK Module for Python.

The MQTT Topic format are as follows:
/point_<point_number>/<parameter>

For example:
/point_1/humidity
/point_2/temperature
/point_2/windspeed
'''

import paho.mqtt.client as mqtt
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

# Your Firebase Database URL here:
projectUrl = "your-project.firebaseio.com"

# Your Firebase JSON credentials here:
credFile = "your-credentials.json"

# Import credentials
cred = credentials.Certificate(credFile)
firebase_admin.initialize_app(cred, {
    'databaseURL': projectUrl,
    'databaseAuthVariableOverride': {
        'uid': 'my-service-worker'
    }
})

# Create reference in database
points = ["point_1", "point_2", "point_3"]
params = ["temp", "humd", "wind"]
refer = {}
topics = []

for point in points:
    for param in params:
        topics.append(point + "/" + param)

for point in points:
    refer.update({
        point: {
            "temp" :db.reference(point+"/temperature/c"),
            "humd" : db.reference(point+"/humidity"),
            "wind" : db.reference(point+"/wind_speed")
        },
        
    })

# Callback for established connection
def on_connect(client, userdata, flags, rc):
    print("Connected with result code: " + str(rc))
    for topic in topics:
        client.subscribe(topic)
    print(refer)

# Callback for received message
def on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    print(message.topic + " " + msg)

    tempc = 0
    humid = 0
    wind = 0

    stationNum = message.topic[6]

    toSend = refer["point_" + stationNum][message.topic[8:len(message.topic)]]
    toSend.set(msg)

# Create client and define callbacks
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Connect to the broker
client.connect("127.0.0.1", 1883, 60)

client.loop_forever()
