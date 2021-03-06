import paho.mqtt.client as mqtt
from time import sleep
from datetime import datetime
import json
import requests
import logging
import base64
import os
import ssl
import uuid

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	print("trying sub")
	rv, mid = client.subscribe("babylon/farm_out", 1)
	print("subscription status: %r" % rv)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	print("Received Message: " + msg.topic+" "+str(msg.payload))

def on_publish(client, userdata, mid):
	print("publish complete! %r" % (mid,) )


def on_disconnect(client, userdata, rc):
	print("mqtt disconnected...")
	client.reconnect()

def on_subscribe(client, userdata, mid, granted_qos):
	print("Successful Subscribe")


client = mqtt.Client(client_id="CLOUD_TEST_" + str(uuid.uuid4().hex)[0:6])
client.on_connect = on_connect
client.on_message = on_message
client.on_publish = on_publish
client.on_disconnect = on_disconnect
client.on_subscribe = on_subscribe

# mqtt+ssl://
# Amazon MQ
# broker_url = "b-d444068d-a1be-4b51-987e-2026a6ba7768-1.mq.us-east-1.amazonaws.com"
# Amazon IOTCore
broker_url = "a37viqgs4xlb64-ats.iot.us-east-1.amazonaws.com"
# broker_url = "a37viqgs4xlb64.iot.us-east-1.amazonaws.com"
# Test Unsecured MQTT
# broker_url = "test.mosquitto.org"
# broker_port = 1883
broker_port = 8883

certRootDir = "/home/skorn/Documents/mqtt_certs/"
rootCAPath = certRootDir + "root-CA.crt"
certificatePath = certRootDir + "pem.crt"
privateKeyPath = certRootDir + "privkey.out"

try:
	with open(certificatePath, "wb") as f:
		# print(os.getenv("AWS_CERT"))
		f.write(base64.b64decode(os.getenv("AWS_CERT")) )

	with open(privateKeyPath, "wb") as f:	
	    f.write(base64.b64decode(os.getenv("AWS_PK")) )

	with open(rootCAPath, "w") as f:
		pem_url = "https://www.amazontrust.com/repository/AmazonRootCA1.pem"
		# pem_url = "https://www.symantec.com/content/en/us/enterprise/verisign/roots/VeriSign-Class%203-Public-Primary-Certification-Authority-G5.pem"		
		f.write(requests.get(pem_url).text)

except Exception as e:
	logger = logging.getLogger('errors')
	print("error getting certs!!!!")
	logger.debug("Messenger Error: Certificate and Private Key ENV Variable Missing! %s", e)


# client.tls_set_context(context=None)
client.tls_set(ca_certs=rootCAPath,certfile=certificatePath, keyfile=privateKeyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_SSLv23)
# client = os.environ["SERIAL_NUM"] + str(uuid.uuid4().hex)[0:6]
# client.username_pw_set("sam", password="testtest1234")
# client.connect(broker_url, broker_port, 60)
rv = client.connect_async(broker_url, broker_port, 60)
print(rv)
# sleep(1)
# client.loop_forever()
client.loop_start()

test_data = {
	'serial_num': '25277782', 'water_valve_on': None, 'water_gpd': 0.0,
	'ph': 7.1, 'irrigation_on': None, 'ph_spd': None, 
	'temp': 65.84, 'humidity': 71.7, 'light_on': None,
	'ec_spd': None, 'co2': None, 'ec': 1677.33, 
	'type': 'sensor', 'message': ''
}

while True:
	# client.loop(0.5)
	ptime = datetime.now().isoformat()
	test_data['message'] = ptime
	test_data['water_gpd'] += 0.1
	client.publish("babylon/farm_out", json.dumps(test_data), qos=1)
	# .wait_for_publish()
	print("published: %s" % ptime)
	sleep(5)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
# client.loop_forever()