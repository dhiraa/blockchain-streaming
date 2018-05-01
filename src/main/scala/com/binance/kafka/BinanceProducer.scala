package com.binance.kafka

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.ws.{Message, TextMessage, WebSocketRequest, WebSocketUpgradeResponse}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink}
import com.softwaremill.react.kafka.{ProducerProperties, ReactiveKafka}

import scala.concurrent.Promise
import akka.stream.scaladsl.Source
import kafka.producer.ReactiveKafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

import com.binance.websocket.data.Schema._
import spray.json._
import scala.util.parsing.json.JSONObject

/**
  * Modified version of
  *   https://github.com/imranshaikmuma/Websocket-Akka-Kafka-Spark
  */
object BinanceProducer {


  def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem()
    implicit val materializer = ActorMaterializer()
    import system.dispatcher

    val kafka = new ReactiveKafka()

    val producer = ReactiveKafkaProducer[Array[Byte], String](
      ProducerProperties(
        bootstrapServers = "localhost:9092",
        topic = "binance",
        valueSerializer = new StringSerializer()
      )
    )

    val flow: Flow[Message, Message, Promise[Option[Message]]] =
      Flow.fromSinkAndSourceMat(
        Sink.foreach[Message](
          record => {
            println(record.asInstanceOf[TextMessage].getStrictText)
            producer.producer.send(
            new ProducerRecord[Array[Byte],String]("binance",
              record
                .asInstanceOf[TextMessage]
                .getStrictText)
          )
          }
        ),
        Source.maybe[Message])(Keep.right)

    //TODO check how this can achieved in other ways
    val (xvgbtcResponse, xvgbtcpromise) =   Http().singleWebSocketRequest(
      WebSocketRequest("wss://stream.binance.com:9443/ws/xvgbtc@trade"),
      flow)

    val (btcusdtResponse, btcusdtPromise) =   Http().singleWebSocketRequest(
      WebSocketRequest("wss://stream.binance.com:9443/ws/btcusdt@trade"),
      flow)


    val tradesResponse = List(xvgbtcResponse, btcusdtResponse) //Rude way for now!

    val connected = tradesResponse.map {
      response =>
        response.map {
          upgrade =>
            if (upgrade.response.status == StatusCodes.SwitchingProtocols) {
              Done
            } else {
              throw new RuntimeException(s"Connection failed: ${upgrade.response.status}")
            }
        }
    }

    connected.foreach(_.onComplete(println))
    //promise.success(None)

  }
}
