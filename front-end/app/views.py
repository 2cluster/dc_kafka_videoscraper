from app import app

from flask import render_template, jsonify, request, redirect, Response
from pykafka import KafkaClient
from bs4 import BeautifulSoup
import requests
import re
import hashlib
from time import sleep
import json
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from datetime import datetime

def getM3U8_1(json_obj):
    return json.loads(json_obj).get("text"), json.loads(json_obj).get("video").get("video_url")

def get_kafka_client():
    return KafkaClient(hosts='localhost:9092')

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
            




@app.route('/')
def index():

    print(app.config)

    return render_template("public/index.html")


@app.route('/load', methods=["GET", "POST"])
def load():

    if request.method == "POST":
        
        url = request.form["url"]
        video_topic = "video2_topic"
        m3u8_topic  = "m3u8_topic"
        ts_topic    = "ts_topic"


        value_schema_main_M3U8 = avro.load('schema/video_v.avsc')
        key_schema_main_M3U8 = avro.load('schema/video_k.avsc')

        value_schema_sub_M3U8 = avro.load('schema/m3u8_v.avsc')
        key_schema_sub_M3U8 = avro.load('schema/m3u8_k.avsc')

        value_schema_TS = avro.load('schema/ts_v.avsc')
        key_schema_TS = avro.load('schema/ts_k.avsc')

        avroProducer_main_M3U8 = AvroProducer(
            {'message.max.bytes' : '15728640', 'bootstrap.servers': '127.0.0.1:9092', 'schema.registry.url': 'http://127.0.0.1:8081'}, 
            default_key_schema=key_schema_main_M3U8, default_value_schema=value_schema_main_M3U8
        )

        avroProducer_sub_M3U8 = AvroProducer(
            {'message.max.bytes' : '15728640', 'bootstrap.servers': '127.0.0.1:9092', 'schema.registry.url': 'http://127.0.0.1:8081'}, 
            default_key_schema=key_schema_sub_M3U8, default_value_schema=value_schema_sub_M3U8
        )

        avroProducer_TS = AvroProducer(
            {'message.max.bytes' : '15728640', 'bootstrap.servers': '127.0.0.1:9092', 'schema.registry.url': 'http://127.0.0.1:8081'}, 
            default_key_schema=key_schema_TS, default_value_schema=value_schema_TS
        )



        page = requests.get(url)
        soup = BeautifulSoup(page.text, 'html.parser')

        for component in soup.findAll('div', class_='component_container video'):
            video_array=[]
            
            title, link = getM3U8_1(component['data-context'])
            
            myfile = requests.get(link)
            open('./m3u8_temp.txt', 'wb').write(myfile.content)
            m3u8_files = getM3U8_2()
            url_base = getBaseURL()
            
            #Hash maken van de title
            title_hash = hashlib.md5(title.encode()) 
            
            
            #Data via producer naar topic M3U8_MAIN sturen
            key_main = {"title": title}            
            value_main = {"title": title, "title_hash": str(title_hash.hexdigest()), "content": myfile.content}
            avroProducer_main_M3U8.produce(topic=video_topic, value=value_main, key=key_main)
            avroProducer_main_M3U8.flush()
                
            x = 0
            for m3u8_2_link in m3u8_files:

                myfile = requests.get(m3u8_2_link)
                open('./ts_temp.txt', 'wb').write(myfile.content)
                array = getTS()
                video_array.append(array)
                
                #Data via producer naar topic M3U8_SUB sturen

                key_sub = {"m3u8_sub_id": str(x), "title": title}            
                value_sub = {"title": str(title_hash.hexdigest()), "sub_m3u8_id": str(x),  "content": myfile.content}
                
                avroProducer_sub_M3U8.produce(topic=m3u8_topic, value=value_sub, key=key_sub)
                avroProducer_sub_M3U8.flush()
                
                x += 1
                
            
            x=0
            for items in video_array: #m3u8 sub loop
                
                
                
                y=0
                
                for item in items: #ts files loop
                    
                    #Data via producer naar topic TS_FILES sturen
                    ts_file = requests.get(url_base + item)
                    key_ts = {"m3u8_id": str(x), "ts_id": str(y), "title": title}            
                    value_ts = {"title": str(title_hash.hexdigest()), "sub_m3u8_id": str(x), "ts_id": str(y),  "content": ts_file.content}
                    avroProducer_TS.produce(topic=ts_topic, value=value_ts, key=key_ts)
                    avroProducer_TS.flush()


                    y+=1
                x+=1
    



        return redirect("/webconsumer/ts_topic")

    return render_template("public/load.html")




@app.route('/content')
def content():

    return render_template("public/content.html")


@app.route('/content/<single>')
def single(single):
    
    return render_template("public/content.html")


@app.route('/webconsumer/<topic>')
def get_messages(topic):
    
    client = get_kafka_client()
    def events():
        for i in client.topics[topic].get_simple_consumer():
            yield '$:{0}\n'.format(i.value)
    return Response(events(), mimetype="text/event-stream")






@app.route('/download/<single>')
def download(single):
    
    return jsonify({"name": "video naam", "content": "awosrcvbecikzfryuifnzskxerjfgzdukghfbjyuigh"})

