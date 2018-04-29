
#How to run?

cd /opt/dhiraa/blockchain_streaming/softwares/kafka_2.11-1.1.0

## Kafka

1. Download Kafka [here](https://kafka.apache.org/downloads)

2. Start the Zookeeper server:

   `
   bin/zookeeper-server-start.sh config/zookeeper.properties
   `
   
3. Start the Kafka Server:

   `
   bin/kafka-server-start.sh config/server.properties
   `
  
4. Start the Kafka Consumer on port 9092 with any topic of choice:

   `
   bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic binance --new-consumer
   `
## Spark Structured Streaming Analytics



## Report
- Tools
    - Spark Streaming
    - Akka
    - Kafka
- Replay
- In production downstream applications

# References:
## Tutorials
- https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html
- http://spark.apache.org/docs/latest/streaming-custom-receivers.html
- https://forums.databricks.com/questions/2095/sparkstreaming-to-process-http-rest-end-point-serv.html
- https://www.supergloo.com/fieldnotes/spark-streaming-example-from-slack/
- https://medium.com/zappos-engineering/creating-a-scalable-websocket-in-an-hour-f7fb217e3038
- https://www.confluent.io/blog/building-a-microservices-ecosystem-with-kafka-streams-and-ksql/
- https://blog.knoldus.com/2016/06/13/a-simple-example-to-implement-websocket-server-using-akka-http/
- https://www.microsoft.com/developerblog/2017/11/01/building-a-custom-spark-connector-for-near-real-time-speech-to-text-transcription/
- https://dzone.com/articles/creating-a-scalable-websocket-application-in-an-ho
- https://www.playframework.com/documentation/2.6.x/ScalaWebSockets
- **https://doc.akka.io/docs/akka-stream-kafka/current/producer.html**

### Git:
- https://gist.github.com/samklr/219c9f5a7c4a3f4853808e5a6d2326b0
- https://github.com/rstml/datastax-spark-streaming-demo
- https://github.com/atemerev/spark-streaming-websocket
- https://github.com/granthenke/spark-demo
- **https://github.com/imranshaikmuma/Websocket-Akka-Kafka-Spark**


# ReadMe
- https://kafka.apache.org/downloads
