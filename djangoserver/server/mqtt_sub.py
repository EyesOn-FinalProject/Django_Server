from datetime import time
from threading import Thread
from django.apps import AppConfig
import paho.mqtt.client as mqtt




class MyMqtt(Thread):
    def __init__(self,client):
        super().__init__()
        self.client = mqtt.Client()

    def run(self):
        self.client.connect("192.168.0.202", 1883, 60)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.loop_forever()

    def on_connect(self, client, userdata, flags, rc):
        print("connect.." + str(rc))
        if rc == 0:
            client.subscribe("eyeson/#")
        else:
            print("연결실패")

    def on_message(self, client, userdata, msg):
        myval = msg.payload.decode("utf-8")
        print(myval)
        data = list(myval.split(","))
        print(data)



class Parksub(AppConfig):
    name = 'server'
    verbose_name = "My App"

    def ready(self):
        print("작업시작")
        client = mqtt.Client()
        mymqtt = MyMqtt(client)
        mymqtt.start()