package com.binance.websocket.data


import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.types.LongType
import java.sql.Timestamp

import org.apache.spark.sql.types.{ArrayType, StringType, StructType, TimestampType}

object Schema {

  /**
  {
    "e": "trade",     // Event type
    "E": 123456789,   // Event time
    "s": "BNBBTC",    // Symbol
    "t": 12345,       // Trade ID
    "p": "0.001",     // Price
    "q": "100",       // Quantity
    "b": 88,          // Buyer order Id
    "a": 50,          // Seller order Id
    "T": 123456785,   // Trade time
    "m": true,        // Is the buyer the market maker?
    "M": true         // Ignore.
  }


  AAPL/CAD
    Where APPL stock is being bought and sold in exchange for Canadian dollars.

  XVGBTC:
    XVG/BTC
    Where XVG is being bought and sold in exchange for BTC.
    If m is true, then the trade is maker of market, who wants to buy XVG with BTC

    XVG_q = q is the number of XVG shares the maker wants to buy
    BTC_p = p is the price of one unit of BTC
    
    BTC_Market = XVG_q * BTC_p

  BTCUSDT:
    BTC/USDT
    Where BTC is being bought and sold in exchange for USDT.
    If m(market maker) is true, then the trade is maker of market, who wants to buy BTC with USDT

    BTC_q = q is the number of BTC shares the maker wants to buy
    USDT_p = p is the price of one unit of USDT
    
    USDT_Market = BTC_q * USDT_p


    To do this, you’ll need to somehow merge the BTC/USDT and the XVG/BTC streams.
  XVG/USDT :
    XVG <--- BTC <--- USDT
    Find indirect market makers who is increasing the sales of XVG with USDT but through BTC.
    i.e Find the market makers who is selling USDT for BTC and BTC for XVG

    +-----+-------------+-------+--------+-------+--------+--------+--------+-------------------+-----+----+
    |e    |E            |s      |t       |p      |q       |b       |a       |T                  |m    |M   |
    +-----+-------------+-------+--------+-------+--------+--------+--------+-------------------+-----+----+
    |trade|1525022020366|BTCUSDT|39474832|9264.25|0.031333|94506681|94506685|2018-04-29 22:43:40|true |true|
    |trade|1525022021635|BTCUSDT|39474833|9263.24|0.089559|94506688|94506686|2018-04-29 22:43:41|false|true|
    |trade|1525022022832|XVGBTC |14773476|7.48E-6|633.0   |33747721|33747986|2018-04-29 22:43:42|true |true|
    |trade|1525022022894|BTCUSDT|39474834|9264.99|0.043242|94506695|94506641|2018-04-29 22:43:42|false|true|
    |trade|1525022023130|BTCUSDT|39474835|9264.78|0.452118|94506693|94506696|2018-04-29 22:43:43|true |true|
    |trade|1525022024957|BTCUSDT|39474836|9264.99|0.026368|94506704|94506641|2018-04-29 22:43:44|false|true|
    |trade|1525022025528|BTCUSDT|39474837|9264.78|0.013156|94506705|94506708|2018-04-29 22:43:45|true |true|
    |trade|1525022025658|BTCUSDT|39474838|9264.99|0.186017|94506710|94506641|2018-04-29 22:43:45|false|true|
    |trade|1525022025658|BTCUSDT|39474839|9264.99|0.036719|94506710|94506643|2018-04-29 22:43:45|false|true|
    |trade|1525022025658|BTCUSDT|39474840|9265.0 |0.170202|94506710|94506568|2018-04-29 22:43:45|false|true|
    |trade|1525022025658|BTCUSDT|39474841|9265.0 |0.298379|94506710|94506603|2018-04-29 22:43:45|false|true|
    |trade|1525022025658|BTCUSDT|39474842|9266.0 |0.055779|94506710|94506624|2018-04-29 22:43:45|false|true|
    |trade|1525022025658|BTCUSDT|39474843|9266.01|0.003137|94506710|94506664|2018-04-29 22:43:45|false|true|
    |trade|1525022026480|XVGBTC |14773477|7.48E-6|189.0   |33747721|33747992|2018-04-29 22:43:46|true |true|
    |trade|1525022026976|BTCUSDT|39474844|9264.78|0.028593|94506705|94506712|2018-04-29 22:43:46|true |true|
    |trade|1525022026995|BTCUSDT|39474845|9264.78|0.092775|94506705|94506713|2018-04-29 22:43:47|true |true|
    |trade|1525022028134|XVGBTC |14773478|7.49E-6|1547.0  |33747993|33747961|2018-04-29 22:43:48|false|true|
    |trade|1525022028154|BTCUSDT|39474846|9265.0 |0.037173|94506715|94506718|2018-04-29 22:43:48|true |true|
    |trade|1525022028846|XVGBTC |14773479|7.48E-6|567.0   |33747721|33747994|2018-04-29 22:43:48|true |true|
    |trade|1525022029055|XVGBTC |14773480|7.49E-6|970.0   |33747995|33747961|2018-04-29 22:43:49|false|true|
    +-----+-------------+-------+--------+-------+--------+--------+--------+-------------------+-----+----+
    **/


  //Schema info to convert incoming Json stream into Spark Dataframes/Dataset
  val tradeStreamsSchema = new StructType()
    .add("e", StringType)
    .add("E", StringType)
    .add("s", StringType)
    .add("t", StringType)
    .add("p", StringType)
    .add("q", StringType)
    .add("b", StringType)
    .add("a", StringType)
    .add("T", TimestampType)
    .add("m", StringType)
    .add("M", StringType)

  //With option parsing the string data becomes little probalamatic! TODO
  //  case class TradeStreams(e: Option[String],
  //                          E: Option[Long],
  //                          s: Option[String],
  //                          t: Option[String],
  //                          p: Option[Double],
  //                          q: Option[Double],
  //                          b: Option[String],
  //                          a: Option[String],
  //                          T: Option[String],
  //                          m: Option[String],
  //                          M: Option[String])

  case class TradeStreams(e: String,
                          E: String,
                          s: String,
                          t: String,
                          p: Double,
                          q: Double,
                          b: String,
                          a: String,
                          T: String,
                          m: String,
                          M: String)


  /**
    * A simple naive function to convert incoming Trade stream line inot Case class
    * Most of the Json library for some reason failed to parse our line of interest.
    *
    * For some reasons all exisiting Scala Json parsers fail to parse our message. Strange!!!
    * @param line incoming Json stream, one line at a time
    * @return
    */
  def parseTradeStreams(line: String):TradeStreams = {

    val res = line
      .replace("{", "")
      .replace("}", "")
      .replace("\"", "")
      .split(",")
      .map(field => {
        val res = field.split(":")
        (res(0).toString, res(1).toString)
      })
      .toMap

    TradeStreams(e = res.getOrElse("e", "0"),
      E = res.getOrElse("E", "0"),
      s = res.getOrElse("s", "0"),
      t = res.getOrElse("t", "0"),
      p = res.getOrElse("p", "0").toDouble,
      q = res.getOrElse("q", "0").toDouble,
      b = res.getOrElse("b","0"),
      a = res.getOrElse("a", "0"),
      T = res.getOrElse("T", "0"),
      m = res.getOrElse("m", "0"),
      M = res.getOrElse("M", "0"))
  }

  //TODO check why it fails to parse E, for which the value is Long not String
  object TradeStreamsProtocol extends DefaultJsonProtocol {
    implicit val tradeStreamsFormat = jsonFormat11(TradeStreams)
  }




}
