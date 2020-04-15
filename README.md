docker exec -it broker bash

kafka-console-consumer --topic video2_topic --bootstrap-server localhost:9092
kafka-console-consumer --topic m3u8_topic --bootstrap-server localhost:9092
kafka-console-consumer --topic ts_topic --bootstrap-server localhost:9092