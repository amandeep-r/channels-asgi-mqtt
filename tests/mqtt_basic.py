import paho.mqtt.client as mqtt
from time import sleep
from datetime import datetime

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("babylon/farm_out/#")

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_publish(client, userdata):
	print("publish complete!")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish

# mqtt+ssl://
# client.tls_set_context(context=None)
broker_url = "b-d444068d-a1be-4b51-987e-2026a6ba7768-1.mq.us-east-1.amazonaws.com"
# broker_url = "test.mosquitto.org"
# broker_port = 1883
broker_port = 8883

client.username_pw_set("sam", password="testtest1234")
client.connect(broker_url, broker_port, 60)
client.loop_start()

while True:
	ptime = datetime.now().isoformat()
	client.publish("babylon/farm_out", ptime, qos=2)
	print("published: %s" % ptime)
	sleep(5)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
# client.loop_forever()