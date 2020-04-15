from bs4 import BeautifulSoup
import requests
import re
from time import sleep
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from pymongo import MongoClient
from json import loads
import urllib.parse


value_schema_main_M3U8 = avro.load('schema/video_v.avsc')
key_schema_main_M3U8 = avro.load('schema/video_k.avsc')

value_schema_sub_M3U8 = avro.load('schema/m3u8_v.avsc')
key_schema_sub_M3U8 = avro.load('schema/m3u8_k.avsc')

value_schema_TS = avro.load('schema/ts_v.avsc')
key_schema_TS = avro.load('schema/ts_k.avsc')

avroConsumer_M3U8_1 = AvroConsumer(
    {'message.max.bytes' : '15728640', 'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://localhost:8081', 'group.id':'my-group'}, 
    reader_key_schema=key_schema_main_M3U8, reader_value_schema =value_schema_main_M3U8
)

avroConsumer_M3U8_2 = AvroConsumer(
    {'message.max.bytes' : '15728640', 'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://localhost:8081', 'group.id':'my-group'}, 
    reader_key_schema=key_schema_sub_M3U8, reader_value_schema=value_schema_sub_M3U8
)

avroConsumer_TS = AvroConsumer(
    {'message.max.bytes' : '15728640', 'bootstrap.servers': 'localhost:9092', 'schema.registry.url': 'http://localhost:8081', 'group.id':'my-group'}, 
    reader_key_schema=key_schema_TS, reader_value_schema=value_schema_TS
)

avroConsumer_M3U8_1.subscribe(["video_topic"])
avroConsumer_M3U8_2.subscribe(["m3u8_topic"])
avroConsumer_TS.subscribe(["ts_topic"])


username = urllib.parse.quote_plus('root')
password = urllib.parse.quote_plus('rootpassword')

client = MongoClient('mongodb://%s:%s@127.0.0.1:27017' % (username, password))
collection_m3u8_main = client.ipbdamh.m3u8_main
collection_m3u8_sub = client.ipbdamh.m3u8_sub
collection_ts_files = client.ipbdamh.ts_files

while True:
    try:
        msg = avroConsumer_M3U8_1.poll(0.001)
        
        

        if msg:
            collection_m3u8_main.insert_one(msg.value())

        msg1 = avroConsumer_M3U8_2.poll(0.001)
        if msg1:
            collection_m3u8_sub.insert_one(msg1.value())

        msg2 = avroConsumer_TS.poll(0.001)
        if msg2:
            collection_ts_files.insert_one(msg2.value())


        

    except SerializerError as e:
        print("Message deserialization failed for {}: {}".format(msg, e))
        break


    





avroConsumer_M3U8_1.close()
avroConsumer_M3U8_2.close()
avroConsumer_TS.close()
    
   
