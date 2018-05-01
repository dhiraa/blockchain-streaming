package com.binance


import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

import com.binance.websocket.data.Schema
import com.binance.RuntimeConfig

case class Stats(name: String = "XVGUSDT",
                 n: Long = 0,
                 vwap: Double = 0.0,
                 sum: Double=0.0,
                 mean: Double = 0.0,
                 std: Double=0.0)

case class VWAP(vwap: Double=0.0)

object StatefullOperations {

  //TODO http://jonisalonen.com/2014/efficient-and-accurate-rolling-standard-deviation/

  /**
    * Spark state update function.
    * For simplicity the statistics are calculated from very first aggregated batch onwards on given batch interval
    * @param newValues Aggregated stream VWAP Eg: (("XVGUSDT",VWAP),("XVGUSDT",VWAP),...)
    * @param runningCount Which holds the current statistics state of the VWAP
    * @return Stats
    */
  def updateFunction(newValues: Seq[VWAP], runningCount: Option[Stats]): Option[Stats] = {
    val vwap = newValues(0).vwap //A strong asumption that we get only one value after all aggregations
    val numEvents = newValues.length + runningCount.getOrElse(new Stats).n
    val sum = runningCount.getOrElse(new Stats).sum + newValues.map(_.vwap).sum
    val mean = sum / numEvents
    val std = Math.sqrt(Math.pow(mean - vwap,2)/(numEvents-1)) //are we doing this right? for first time it will go for Nan

    Some(Stats(n=numEvents, vwap=vwap, mean = mean, sum = sum, std = std))
  }

}

object Streaming {

  def main(args: Array[String]): Unit = {

    val conf = new RuntimeConfig(args)
    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive" , "True")
      .config("spark.sql.streaming.checkpointLocation","/tmp/blockchain-streaming/sql-streaming-checkpoint")
      .master("local[4]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(conf.batchTimeInSeconds.getOrElse(30)))
    ssc.checkpoint("/tmp/blockchain-streaming/streaming-checkpoint")

    // https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "0",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("binance")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent, //LocationStrategies for efficient computation
      Subscribe[String, String](topics, kafkaParams)
    )

    val vwap = stream
      .map(record => record.value) //Json string {"e":"trade","E":1525152441625,"s":"XVGBTC","t":15001011,"p":"0.00000845","q":"2074.00000000","b":34334307,"a":34335232,"T":1525152441621,"m":true,"M":true}
      .filter(record => !record.isEmpty)
      .map(record => Schema.parseTradeStreams(record)) //TradeStreams
      .map(tradeStreamsFormat => (tradeStreamsFormat.s,
                (tradeStreamsFormat.p * tradeStreamsFormat.q, tradeStreamsFormat.q))) //(XVGBTC, (pq,q)) or (BTCUSDT, (pq,q))
      .reduceByKey((value1, value2) =>
          (value1._1+value2._1, value1._2+value2._2)
        ) //(XVGBTC, (sum(pq),sum(q))) or (BTCUSDT, (sum(pq),sum(q)))
      .map(record => (record._1, record._2._1 / record._2._2)) //(XVGBTC, sum(pq)/sum(q)) or (BTCUSDT, sum(pq)/sum(q))


    //TODO what happens if there is a missing trade for one of the streams? BOOM!!!
    val combinedVwap= vwap
      .map(record => ("XVGUSDT", record._2)) //(XVGUSDT, sum(pq)/sum(q))
      .reduceByKey((value1, value2) => value1 * value2)
      .map(record => (record._1, VWAP(vwap = record._2))) //("XVGUSDT",VWAP)

    val std = combinedVwap //("XVGUSDT",VWAP)
      .updateStateByKey(StatefullOperations.updateFunction) //(XVGUSDT,Stats(XVGUSDT,0.0,6,17849.987084185683,0.0))
      .map(_._2) //Stats(XVGUSDT,0.0,6,17849.987084185683,0.0)

    stream.map(record=>(record.value().toString)).print()
    vwap.print()
    combinedVwap.print()
    std.print()

    vwap.saveAsTextFiles("/tmp/blockchain-streaming/binance/vwap/")
    std.saveAsTextFiles("/tmp/blockchain-streaming/binance/std/")

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
