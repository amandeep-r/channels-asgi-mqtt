import os
import asyncio
import functools
import logging
import time
import signal
import json

import paho.mqtt.client as mqtt
import base64
import uuid

import requests
import ssl

logger = logging.getLogger('mqtt')

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

		self.client_id = "CLOUD_SERVER_" + str(uuid.uuid4().hex)[0:6] #NOT farm serial number

		self.client = mqtt.Client(
			client_id = self.client_id,
			userdata = {
				"server": self,
				"channel": self.channel,
				"host": self.host,
				"port": self.port,
			}
		)
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


		self.client.tls_set(
			ca_certs=self.rootCAPath,
			certfile=self.certificatePath,
			keyfile=self.privateKeyPath,
			cert_reqs=ssl.CERT_REQUIRED,
			tls_version=ssl.PROTOCOL_SSLv23
		)
		self.topics_subscription = topics_subscription or [("#", 2),]
		assert isinstance(self.topics_subscription, list), "Topic subscription must be a list with (topic, qos)"

		self.mqtt_channel_name = mqtt_channel_name or "mqtt"
		self.mqtt_channel_pub = mqtt_channel_pub or "mqtt.pub"
		self.mqtt_channel_sub = mqtt_channel_sub or "mqtt.sub"

		self.logger.debug(f"Channel: {channel}, mqtt_channel: {self.mqtt_channel_name}, mqtt_pub: {self.mqtt_channel_pub}")

	def configureCertFiles(self):
		try:
			with open(self.certRootDir + self.certificatePath, "wb") as f:
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

	# Receiving message
	def _mqtt_send_got_result(self, future):
		self.logger.debug("Receiving message from MQTT broker.")
		result = future.result()
		if result:
			self.logger.debug("Result: %s", result)

	def _on_message(self, client, userdata, message):
		self.logger.debug("Received message from topic {}".format(message.topic))
		payload = message.payload.decode("utf-8")

		try:
			payload = json.loads(payload)
		except Exception as e:
			self.logger.debug("Payload is not JSON Serializable: %r", e)
		
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
			# create a coroutine and receive?
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

	def _mqtt_receive(self, msg):
		"""
		Receive a message from Channel `mqtt.pub` and send it to MQTT broker
		"""
		self.logger.info(f"Receive raw messages:\r\n{msg} \r\nType: {msg['type']}")
		# We only listen for messages from mqtt_channel_pub
		if msg['type'] == self.mqtt_channel_pub:
			try:
				payload = msg['text']
				if not isinstance(payload, dict):
					self.logger.debug("For some reason payload isn't a dictionary!")
					payload = json.loads(payload)

				topic =  payload['topic']
				data = payload['payload']
				self.logger.info(f"Send a MQTT message with payload:\r\n{payload}")

				rc, mid = self.client.publish(
						topic,
						data,
						qos=payload.get('qos', 1), 
						retain=False)
				self.logger.debug(f"MQTT MSG Publish Status: {rc},{mid}")
			except Exception as e:
				self.logger.error(f"Error publishing! {e}")

	async def client_pool_message(self):
		self.logger.debug("Inside Send Pool Start!")
		self.logger.debug(asyncio.all_tasks())

		# Test to make sure send actually works, before channels are involved
		# for i in range(0,8):
		# 	rc, mid = self.client.publish(
		# 		'testmq/2',
		# 		json.dumps({"hi":f"there: {i}"}),
		# 		qos=1,
		# 		retain=False
		# 	)
		# 	self.logger.debug(f"Test mqtt msg sent: {rc}, id: {mid}")

		while True:
			# break # break pulling messages from channel
			self.logger.info("Wait for a message from channel %s", self.mqtt_channel_name)
			# self._mqtt_receive(await self.channel.receive(self.mqtt_channel_name))
			# self.logger.info("Received a message in channel %s", self.mqtt_channel_name)
			result = await self.channel.receive('mqttout')
			# result = await self.channel.receive(self.mqtt_channel_name)
			self._mqtt_receive(result)
			await asyncio.sleep(0.1)

	async def client_pool_start(self):
		"""
		This is the main loop pool for receiving MQTT messages
		"""
		self.logger.debug("Inside Receive Pool Start!")
		self.logger.debug(asyncio.all_tasks())
		if self.username:
			self.logger.info("Connecting with Username: Using TLS!!!")
			self.client.tls_set_context(context=None)
			self.client.username_pw_set(username=self.username, password=self.password)
		
		rv = self.client.connect(self.host, self.port)
		# rv = client.connect_async(broker_url, broker_port, 60)

		self.logger.debug("Connection Status: %r", rv)
		self.logger.info("Starting loop")

		while True:
			self.client.loop(0.1)
			await asyncio.sleep(0.1)
			# Check for MQTT Messages
			# self.logger.debug("Restarting loop")


	def stop_server(self, signum):
		self.logger.info("Received signal {}, terminating".format(signum))
		self.stop = True
		for task in asyncio.Task.all_tasks():
			task.cancel()
		self.loop.stop()


	def run(self):
		self.stop = False
		# loop = asyncio.new_event_loop()
		# asyncio.set_event_loop(loop)
		loop = asyncio.get_event_loop()
		self.loop = loop

		# Catch signals and stop server
		for signame in ('SIGINT', 'SIGTERM'):
			self.loop.add_signal_handler(
					getattr(signal, signame),
					functools.partial(self.stop_server, signame)
				)

		self.logger.info("Event loop running forever, press Ctrl+C to interrupt.")
		self.logger.info("pid %s: send SIGINT or SIGTERM to exit." % os.getpid())

		tasks = asyncio.gather(*[
				asyncio.ensure_future(self.client_pool_start()),
				asyncio.ensure_future(self.client_pool_message()),
			])

		asyncio.wait(tasks)

		# tasks = [asyncio.create_task(self.client_pool_start()), asyncio.create_task(self.client_pool_message() ) ]
		# asyncio.gather(*tasks)
		# # ensure_future == create_task
		# # task = asyncio.create_task(coro())

		# # Run Tasks concurrently
		# asyncio.wait(*tasks)

		try:
			# Run the event loop until stop() is called.
			loop.run_forever()
		finally:
			loop.run_until_complete(loop.shutdown_asyncgens())
			loop.close()
			self.logger.info("Successfully stopped event loop. Disconnecting from MQTT.")
		
		self.client.disconnect()
