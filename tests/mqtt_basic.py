import paho.mqtt.client as mqtt
from time import sleep
from datetime import datetime
import json

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    # client.subscribe("babylon/farm_out", 1)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_publish(client, userdata, mid):
	print("publish complete! %r" % (mid,) )


def on_disconnect(client, userdata, rc):
	print("mqtt disconnected...")


client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.on_disconnect = on_disconnect

# mqtt+ssl://
client.tls_set_context(context=None)
broker_url = "b-d444068d-a1be-4b51-987e-2026a6ba7768-1.mq.us-east-1.amazonaws.com"
# broker_url = "test.mosquitto.org"
# broker_port = 1883
broker_port = 8883

client.username_pw_set("sam", password="testtest1234")
client.connect(broker_url, broker_port, 60)
sleep(1)
client.loop_start()

test_data = {
	'serial_num': '25277782', 'water_valve_on': None, 'water_gpd': 0.0,
	'ph': 7.1, 'irrigation_on': None, 'ph_spd': None, 
	'temp': 65.84, 'humidity': 71.7, 'light_on': None,
	'ec_spd': None, 'co2': None, 'ec': 1677.33, 
	'type': 'sensor', 'message': ''
}

while True:
	ptime = datetime.now().isoformat()
	test_data['message'] = ptime
	test_data['water_gpd'] += 0.25
	client.publish("babylon/farm_out", json.dumps(test_data), qos=2).wait_for_publish()
	print("published: %s" % ptime)
	sleep(5)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
# client.loop_forever()