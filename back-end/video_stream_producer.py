#!/usr/bin/env python
# coding: utf-8

from bs4 import BeautifulSoup
import requests
import re
from time import sleep
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

url = input("geef de url van de video pagina: ")

topic_name = input("geef de naam van de topic: ")

value_schema = avro.load('schema/ValueSchema.avsc')
key_schema = avro.load('schema/KeySchema.avsc')


avroProducer = AvroProducer(
    {'message.max.bytes' : '15728640', 'bootstrap.servers': '127.0.0.1:9092', 'schema.registry.url': 'http://127.0.0.1:8081'}, 
    default_key_schema=key_schema, default_value_schema=value_schema
)


def getM3U8_1(json_obj):
    return json.loads(json_obj).get("text"), json.loads(json_obj).get("video").get("video_url")



def getM3U8_2():
    f = open("m3u8_temp.txt", "r")
    allM3U8 = []
    for line in f:
        regex = re.compile(r'^(?:http|ftp)s?://', re.IGNORECASE)
        if(re.match(regex, line)):
            allM3U8.append(line.rstrip("\n"))
    return allM3U8


def getTS():
    f = open("ts_temp.txt", "r")
    allTS = []
    for line in f:
        regex = re.compile(r'(?:[!#])s?', re.IGNORECASE)
        if(re.match(regex, line)):
            continue
        else:
            allTS.append(line.rstrip("\n"))
            

    return allTS



def getBaseURL():
    f = open("m3u8_temp.txt", "r")
    for line in f:
        regex = re.compile(r'^(?:http|ftp)s?://', re.IGNORECASE)
        if(re.match(regex, line)):
            
            url = line.split("/videos/")[0]
            url+="/videos/"
            
            return url




page = requests.get(url)
soup = BeautifulSoup(page.text, 'html.parser')
for component in soup.findAll('div', class_='component_container video'):
    video_array=[]
    
    title, link = getM3U8_1(component['data-context'])
    
    myfile = requests.get(link)
    open('./m3u8_temp.txt', 'wb').write(myfile.content)
    m3u8_files = getM3U8_2()
    url_base = getBaseURL()
    for m3u8_2_link in m3u8_files:

        myfile = requests.get(m3u8_2_link)
        open('./ts_temp.txt', 'wb').write(myfile.content)
        array = getTS()
        video_array.append(array)
        
    
    x=0
    for items in video_array:
        y=0
        for item in items:
            ts_file = requests.get(url_base + item)
            key = {"m3u8_id": str(x), "ts_id": str(y), "title": title}            
            value = {"title": title, "m3u8_id": str(x), "ts_id": str(y),  "content": ts_file.content}
            avroProducer.produce(topic=topic_name, value=value, key=key)
            avroProducer.flush()


            y+=1
        x+=1
    





