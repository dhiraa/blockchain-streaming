package com.binance

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import com.binance.websocket.data.Schema._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.functions._

import com.binance.spark.VWAPCombiner

object StructuredStreaming {

  def main(args: Array[String]): Unit = {

    val vwapCombiner = new VWAPCombiner()
    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive" , "True")
      .config("spark.sql.streaming.checkpointLocation","checkpoints")
      .master("local[4]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val tradeStream = spark
      .readStream
      .format("kafka")
      .option("subscribe", "binance")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .load
      .select('value cast "string" as "json")
      .select(from_json($"json", tradeStreamsSchema) as "data")
      .select("data.*")
      .withColumn("p",  $"p" cast "double")
      .withColumn("q",  $"q" cast "double")
      .withColumn("pq", $"p" * $"q")
      .withColumn("T", from_unixtime(($"T" cast "bigint")/1000).cast(TimestampType))
      //      .withColumn("T", unix_timestamp($"T", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
      .withWatermark("T", "5 seconds")
      .as[TradeStreams]


    tradeStream.printSchema()

    tradeStream.createOrReplaceTempView("trade_stream")

    val vwap = tradeStream
      .groupBy("s")
      .agg(
        (sum("pq")/sum("q")).alias("value").cast(StringType)
      )
      .withColumn("dummy", lit("0").cast(StringType))


    vwap
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()


//    vwap.writeStream.format("kafka")
//      .outputMode("complete")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic","vwap")
//      .option("checkpointLocation", "src/main/kafkasink/chkpoint")
//      .start()
//
//
//    spark
//      .readStream
//      .format("kafka")
//      .option("subscribe","vwap")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .load
//      .select('value cast "string" as "vwap")
//      .writeStream
////      .outputMode("complete")
//      .format("console")
//      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
//      .option("truncate",false)
//      .start()
//      .awaitTermination()

    /**

    //Multiple streaming aggregations are not supported with streaming DataFrames/Datasets;;

    val xvgBTC_ = tradeStream
      .withWatermark("T", "5 seconds")
      .filter('s === "XVGBTC")
      .withColumn("pq",  tradeStream("p") * tradeStream("q"))
      .createOrReplaceTempView("xvgBTC")
    //      .groupBy("s").sum("pq")
    //      .groupBy(
    //        window($"T", "10 seconds", "5 seconds"),
    //        $"s"
    //      ).sum("pq")

    import org.apache.spark.sql.Dataset
    val sql = "SELECT s as s1, SUM(pq) as sum_pq, SUM(q) as sum_q FROM xvgBTC GROUP BY s"

    val xvgBTC = spark
      .sql(sql)
      .withColumn("xvgBTC_vwap", $"sum_pq" / $"sum_q")


    val btcUSDT = tradeStream
      .withWatermark("T", "5 seconds")
      .filter('s === "BTCUSDT")
      .withColumn("pq", tradeStream("p") * tradeStream("q"))
      .groupBy("s")
      .sum("pq", "q")
      .withColumn("btcUSDT_vwap", $"sum(pq)" / $"sum(q)")

    val rollingXVGUSDTVwap = xvgBTC
      .join(btcUSDT) //Error: Multiple streaming aggregations are not supported with streaming DataFrames/Datasets;;

    btcUSDT
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()

    xvgBTC
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start().awaitTermination()

    btcUSDT
      .writeStream
      .outputMode("Append")
      .format("json")
      .option("path", "/tmp/binance1/")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()

    xvgBTC
      .writeStream
      .outputMode("Append")
      .format("json")
      .option("path", "/tmp/binance2/")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()
      .awaitTermination()

    rollingXVGUSDTVwap
      .writeStream
      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()

      */

  }
}


