package com.nextgen

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import com.nextgen.websocket.data.Protocol._

import scala.util.parsing.json.JSONObject
import org.apache.spark.sql._
import org.apache.spark.sql.types.TimestampType

object BinanceAnalytics {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive" , "True")
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
      .withColumn("T", unix_timestamp($"T", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").cast(TimestampType))
      .withWatermark("T", "5 seconds")
      .as[TradeStreams]

    tradeStream.printSchema()

    val vwap = tradeStream
      .filter('m === "true")
//      .groupBy(
//      window($"T", "10 seconds", "5 seconds"),
//      $"p"
//    ).sum()


    vwap
      .writeStream
//      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(20, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()
      .awaitTermination()
  }
}


