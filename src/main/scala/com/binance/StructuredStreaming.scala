package com.binance

import java.util.concurrent.TimeUnit

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import com.binance.websocket.data.Schema._
import org.apache.spark.sql.types.{StringType, TimestampType}
import org.apache.spark.sql.functions._
import com.binance.spark.VWAPCombiner

/**
  * //https://databricks.com/blog/2017/04/26/processing-data-in-apache-kafka-with-structured-streaming-in-apache-spark-2-2.html
  */
object StructuredStreaming {

  def toConsole(df: DataFrame, intervalSeconds: Long) = {
    df
      .writeStream
      //      .outputMode("complete")
      .format("console")
      .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()
  }

  def aggDfToConsole(df: DataFrame, intervalSeconds: Long, is_last: Boolean = false) = {

    if (is_last) {
      df
        .writeStream
        .outputMode("complete")
        .format("console")
        .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
        .option("truncate",false)
        .start()
        .awaitTermination()
    } else {
      df
        .writeStream
        .outputMode("complete")
        .format("console")
        .trigger(Trigger.ProcessingTime(intervalSeconds, TimeUnit.SECONDS))
        .option("truncate",false)
        .start()
    }

  }
  def main(args: Array[String]): Unit = {

    val conf = new RuntimeConfig(args)
    val batchTimeInSeconds = conf.batchTimeInSeconds.getOrElse(10)

    val vwapCombiner = new VWAPCombiner()
    val spark = SparkSession
      .builder
      .appName("BinanceStreaming")
      .config("spark.sql.caseSensitive" , "True")
      .config("spark.sql.streaming.checkpointLocation","/tmp/blockchain-streaming/sql-streaming-checkpoint")
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
    //          .as[TradeStreams] //Enable to do any Dataset operations

    tradeStream.printSchema()

    tradeStream.createOrReplaceTempView("trade_stream")


    val sql = "SELECT s as key, SUM(pq) as sum_pq, SUM(q) as sum_q FROM trade_stream GROUP BY s"

    val vwapOverSqlStatement = spark
      .sql(sql)
      .withColumn("vwapOverSqlStatement", $"sum_pq" / $"sum_q")
      .withColumn("value", $"vwapOverSqlStatement".cast("string"))

    vwapOverSqlStatement.printSchema()

    val vwap = tradeStream
      .groupBy(
        window($"T", "10 seconds", "5 seconds"),
        $"s"
      ).sum("pq", "q")

    //(run-main-1) org.apache.spark.sql.AnalysisException: Multiple streaming aggregations are not supported with streaming DataFrames/Datasets;;
    //.groupBy("s")
    //.sum("sum(pq)", "sum(q)")

    aggDfToConsole(vwapOverSqlStatement, batchTimeInSeconds)
    aggDfToConsole(vwap, batchTimeInSeconds, is_last = false)

    vwapOverSqlStatement
      .writeStream
      .format("kafka")
      .outputMode("complete")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic","vwap")
      .option("checkpointLocation", "/tmp/blockchain-streaming/sql-streaming-checkpoint/vwap/")
      .start()

    spark
      .readStream
      .format("kafka")
      .option("subscribe","vwap")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .load
      .select('value cast "string" as "vwapFromSparkStreaming")
      .writeStream
//      .outputMode("complete")
      .format("console")
      .trigger(Trigger.Continuous(batchTimeInSeconds, TimeUnit.SECONDS))
      .option("truncate",false)
      .start()
      .awaitTermination()
  }
}


