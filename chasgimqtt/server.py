import os
import asyncio
import functools
import logging
import time
import signal
import json

from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
from AWSIoTPythonSDK.exception import AWSIoTExceptions

import paho.mqtt.client as mqtt
import base64
import uuid

import requests

logger = logging.getLogger(__name__)

async def mqtt_send(future, channel_layer, channel, event):
    result = await channel_layer.send(channel, event)
    future.set_result(result)

async def mqtt_group_send(future, channel_layer, group, event):
    result  = await channel_layer.group_send(group, event)
    future.set_result(result)

# Only for groups
async def mqtt_group_add(future, channel_layer, group):
    channel_layer.channel_name = channel_layer.channel_name or await channel_layer.new_channel()
    result = await channel_layer.group_add(group, channel_layer.channel_name)
    future.set_result(result)

# Only for groups
async def mqtt_group_discard(future, channel_layer, group):
    result = await channel_layer.group_discard(group, channel_layer.channel_name)
    future.set_result(result)


class Server(object):
    def __init__(self, channel, host, port, username=None, password=None, 
            client_id=None, topics_subscription=None, mqtt_channel_name = None, 
            mqtt_channel_sub=None, mqtt_channel_pub=None):


        self.logger = logging.getLogger("mqtt")
        self.channel = channel
        self.host = host
        self.port = port
        # self.client_id = client_id

        self.clientId = "CLOUD_SERVER_" + str(uuid.uuid4().hex)[0:6] #NOT farm serial number

        self.client = AWSIoTMQTTClient(self.clientId)

        self.client = mqtt.Client(client_id=self.client_id, userdata={
            "server": self,
            "channel": self.channel,
            "host": self.host,
            "port": self.port,
        })
        self.username = username
        # Using certfiles instead of username/password
        self.username = None
        self.password = password
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

        self.certRootDir = "/usr/src/app/mqtt_certs/"
        self.rootCAPath = "root-CA.crt"
        self.certificatePath = "pem.crt"
        self.privateKeyPath = "privkey.out"
        self.configureCertFiles()

        # self.client.configureEndpoint(self.host, self.port)
        # self.client.configureCredentials(self.rootCAPath, self.privateKeyPath, self.certificatePath)

        # self.client.configureAutoReconnectBackoffTime(1, 32, 20)
        # self.client.configureOfflinePublishQueueing(-1)  # Infinite offline Publish self.in_topicqueueing
        # self.client.configureDrainingFrequency(2)  # Draining: 2 Hz
        # self.client.configureConnectDisconnectTimeout(10)  # 10 sec
        # self.client.configureMQTTOperationTimeout(5)  # 5 sec

        # self.client.tls_set(ca_certs=self.rootCAPath, certfile=self.certificatePath, keyfile=self.privateKeyPath)
        self.client.tls_set(ca_certs=self.rootCAPath,certfile=self.certificatePath, keyfile=self.privateKeyPath, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_SSLv23)
        self.topics_subscription = topics_subscription or [("#", 2),]
        assert isinstance(self.topics_subscription, list), "Topic subscription must be a list with (topic, qos)"

        self.mqtt_channel_name = mqtt_channel_name or "mqtt"
        self.mqtt_channel_pub = mqtt_channel_pub or "mqtt.pub"
        self.mqtt_channel_sub = mqtt_channel_sub or "mqtt.sub"


    def configureCertFiles(self):
        try:
            with open(self.certRootDir + self.certificatePath, "wb") as f:
                # print(os.getenv("AWS_CERT"))
                f.write(base64.b64decode(os.getenv("AWS_CERT")) )

            with open(self.certRootDir + self.privateKeyPath, "wb") as f:
                f.write(base64.b64decode(os.getenv("AWS_PK")) )

            with open(self.certRootDir + self.rootCAPath, "w") as f:
                pem_url = "https://www.amazontrust.com/repository/AmazonRootCA1.pem"
                # pem_url = "https://www.symantec.com/content/en/us/enterprise/verisign/roots/VeriSign-Class%203-Public-Primary-Certification-Authority-G5.pem"     
                f.write(requests.get(pem_url).text)
        except Exception as e:
            self.logger.debug("Messenger Error: Certificate and Private Key ENV Variable Missing! %s", e)

    def _on_connect(self, client, userdata, flags, rc):
        self.logger.info("Connected with status {}".format(rc))
        self.logger.info("Subscribing to: %r" % self.topics_subscription)
        rv, test = client.subscribe(self.topics_subscription,1,self._on_message)
        self.logger.debug("Subscribe Status: %r" % rv)


    def _on_disconnect(self, client, userdata, rc):
        self.logger.info("Disconnected")
        if not self.stop:
            j = 3
            for i in range(j):
                self.logger.info("Trying to reconnect")
                try:
                    client.reconnect()
                    self.logger.info("Reconnected")
                    break
                except Exception as e:
                    if i < j:
                        self.logger.warn(e)
                        time.sleep(1)
                        continue
                    else:
                        raise

    def _mqtt_send_got_result(self, future):
        self.logger.debug("Sending message to MQTT channel.")
        result = future.result()
        if result:
            self.logger.debug("Result: %s", result)

    def _on_message(self, client, userdata, message):
        self.logger.debug("Received message from topic {}".format(message.topic))
        payload = message.payload.decode("utf-8")

        try:
            payload = json.loads(payload)
        except Exception as e:
            self.logger.debug("Payload is nos a JSON Serializable: %r", e)
        
        self.logger.debug("Raw message {}".format(payload))

        # Compose a message for Channel with raw data received from MQTT
        msg = {
            "topic": message.topic,
            "payload": payload,
            "qos": message.qos,
            "host": userdata["host"],
            "port": userdata["port"],
        }

        try:
            # create a coroutine and send
            future = asyncio.Future()
            asyncio.ensure_future(
                    mqtt_send(
                        future, 
                        self.channel, 
                        self.mqtt_channel_name,
                        {
                            "type": self.mqtt_channel_sub,
                            "text": msg
                        })
                )

            # attach callback for logs only
            future.add_done_callback(self._mqtt_send_got_result)

        except Exception as e:
            self.logger.error("Cannot send message {}".format(msg))
            self.logger.exception(e)


    async def client_pool_start(self):
        """
        This is the main loop pool for receiving MQTT messages
        """
        self.logger.debug("Inside Client Pool Start!")
        if self.username:
            self.logger.info("Connecting with Username: Using TLS!!!")
            self.client.tls_set_context(context=None)
            self.client.username_pw_set(username=self.username, password=self.password)
        
        rv = self.client.connect(self.host, self.port)

        self.logger.debug("Connection Status: %r", rv)
        self.logger.info("Starting loop")

        while True:
            self.client.loop(0.1)
            self.logger.debug("Restarting loop")
            await asyncio.sleep(0.1)


    def _mqtt_receive(self, msg):
        """
        Receive a menssaje from Channel `mqtt.pub` and send it to MQTT broker
        """
        self.logger.info("Receive raw messages:\r\n%s", msg)
        # We only listen for messages from mqtt_channel_pub
        if msg['type'] == self.mqtt_channel_pub:

            payload = msg['text']

            if not isinstance(payload, dict):
                payload = json.loads(payload)

            self.logger.info("Receive a menssage with payload:\r\n%s", msg)
            self.client.publish(
                    payload['topic'], 
                    payload['payload'], 
                    qos=payload.get('qos', 2), 
                    retain=False)


    async def client_pool_message(self):
        self.logger.info("Loop for messages pool")

        while True:
            self.logger.info("Wait for a message from channel %s", self.mqtt_channel_name)
            self._mqtt_receive(await self.channel.receive(self.mqtt_channel_name))

    def stop_server(self, signum):
        self.logger.info("Received signal {}, terminating".format(signum))
        self.stop = True
        for task in asyncio.Task.all_tasks():
            task.cancel()
        self.loop.stop()


    def run(self):
        self.stop = False
        loop = asyncio.get_event_loop()
        self.loop = loop

        for signame in ('SIGINT', 'SIGTERM'):
            loop.add_signal_handler(
                    getattr(signal, signame),
                    functools.partial(self.stop_server, signame)
                )

        print("Event loop running forever, press Ctrl+C to interrupt.")
        print("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())

        tasks = asyncio.gather(*[
                asyncio.ensure_future(self.client_pool_start()),
                asyncio.ensure_future(self.client_pool_message()),
            ])

        asyncio.wait(tasks)

        try:
            loop.run_forever()
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())            
            loop.close()
            self.logger.info("Successfully stopped event loop. Disconnecting from MQTT.")
        
        self.client.disconnect()
