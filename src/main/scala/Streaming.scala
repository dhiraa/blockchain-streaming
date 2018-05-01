import com.binance.websocket.data.Schema
import com.binance.websocket.data.Schema.TradeStreams
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import spray.json._
import com.binance.websocket.data.Schema.TradeStreamsProtocol._

case class Stats(name: String = "XVGUSDT", n: Long = 0, sum: Double=0.0, mean: Double = 0.0, std: Double=0.0)
case class VWAP(vwap: Double=0.0)

object StatefullOperations {

  //http://jonisalonen.com/2014/efficient-and-accurate-rolling-standard-deviation/

  def updateFunction(newValues: Seq[VWAP], runningCount: Option[Stats]): Option[Stats] = {
    val numEvents = newValues.length + runningCount.getOrElse(new Stats).n
    val sum = runningCount.getOrElse(new Stats).sum + newValues.map(_.vwap).sum
    val mean = sum / numEvents
    val std = (mean - newValues(0).vwap)/(numEvents-1)

    Some(Stats(n=numEvents, mean = mean, sum = sum, std = std))
  }

}
object Streaming {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive" , "True")
      .config("spark.sql.streaming.checkpointLocation","/tmp/streaming/checkpoint")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val ssc = new StreamingContext(spark.sparkContext, Seconds(60))
    ssc.checkpoint("/tmp/streaming/checkpoint")

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
      PreferConsistent,
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
      //      .reduce((record1, record2) => ("XVGUSDT",record1._2 * record2._2))
      .map(record => (record._1, VWAP(vwap = record._2))) //("XVGUSDT",VWAP)

    val std = combinedVwap //("XVGUSDT",VWAP)
      .updateStateByKey(StatefullOperations.updateFunction) //(XVGUSDT,Stats(XVGUSDT,0.0,6,17849.987084185683,0.0))
      .map(_._2) //Stats(XVGUSDT,0.0,6,17849.987084185683,0.0)

    stream.map(record=>(record.value().toString)).print
    vwap.print()
    combinedVwap.print()
    std.print()






    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}
