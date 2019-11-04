import datetime
from asgiref.sync import async_to_sync
from channels.consumer import SyncConsumer
from mqtt_app.models import MQTTdata

from channels.db import database_sync_to_async

# async def connect(self):
# 	self.username = await database_sync_to_async(self.get_name)()

# def get_name(self):


# def addMessage(self):
# 	return MQTTdata.objects.all()[0].name


class MqttConsumer(SyncConsumer):

	def mqtt_sub(self, event):
		print(event)
		topic = event['text']['topic']
		payload = event['text']['payload']
		# do something with topic and payload
		hi = MQTTdata(topic=topic, payload=payload)
		print(f"topic: {topic}, payload: {payload}")
		hi.save()


	def mqtt_pub(self, event):
		topic = event['text']['topic']
		payload = event['text']['payload']
		# do something with topic and payload
		print(f"topic: {topic}, payload: {payload}")
