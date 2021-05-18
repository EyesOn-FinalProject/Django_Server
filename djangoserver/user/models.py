from django.db import models


class User(models.Model):
    object = models.Manager()
    userId = models.AutoField(primary_key=True)

