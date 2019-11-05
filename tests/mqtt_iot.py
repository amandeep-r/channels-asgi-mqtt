from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception import AWSIoTExceptions

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
    # client.subscribe("babylon/farm_out", 1)

# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_publish(client, userdata, mid):
	print("publish complete! %r" % (mid,) )


def on_disconnect(client, userdata, rc):
	print("mqtt disconnected...")



host = "a37viqgs4xlb64.iot.us-east-1.amazonaws.com"
clientId = "IOT_TEST_" + str(uuid.uuid4().hex)[0:6] #NOT farm serial number
port = 8883

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
            f.write(requests.get(
                "https://www.symantec.com/content/en/us/enterprise/verisign/roots/VeriSign-Class%203-Public-Primary-Certification-Authority-G5.pem").text)
except Exception as e:
    logger = logging.getLogger('errors')
    print("error getting certs!!!!")
    logger.debug("Messenger Error: Certificate and Private Key ENV Variable Missing! %s", e)

mqtt_client = AWSIoTMQTTClient(clientId)
mqtt_client.configureEndpoint(host, port)
mqtt_client.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message
mqtt_client.on_publish = on_publish
mqtt_client.on_disconnect = on_disconnect

mqtt_client.configureAutoReconnectBackoffTime(1, 32, 20)
mqtt_client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish self.in_topicqueueing
mqtt_client.configureDrainingFrequency(2)  # Draining: 2 Hz
mqtt_client.configureConnectDisconnectTimeout(10)  # 10 sec
mqtt_client.configureMQTTOperationTimeout(5)  # 5 sec

mqtt_client.connect()
mqtt_client.subscribe("babylon/farm_out",1,on_message)

while True:
	print("mqtt still alive!")
	mqtt_client.publish("babylon/farm_out","test2", 1)
	sleep(15)