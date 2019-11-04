from django.db import models

class MQTTdata(models.Model):
	topic = models.CharField(max_length=30)
	payload = models.CharField(max_length=30)

# Create your models here.
