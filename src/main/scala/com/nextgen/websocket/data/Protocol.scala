package com.nextgen.websocket.data

import java.sql.Timestamp

import scala.reflect.internal.util.Statistics.Quantity
import spray.json._
import DefaultJsonProtocol._
import org.apache.spark.sql.types.LongType
//import java.sql.Timestamp

import org.apache.spark.sql.types.{ArrayType, StringType, StructType, TimestampType} // if you don't supply your own Protocol (see below)

object Protocol {

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


  {"e":"trade","E":1524828423852,"s":"XVGBTC","t":14637260,"p":"0.00000786","q":"202.00000000",
  "b":33387425,"a":33387628,"T":1524828423848,"m":true,"M":true}

  {"e":"trade","E":1524828443994,"s":"BTCUSDT","t":38807938,"p":"9325.00000000","q":"0.11300400",
  "b":93225999,"a":93225997,"T":1524828443992,"m":false,"M":true}

  {"e":"trade","E":1524828444911,"s":"BTCUSDT","t":38807939,"p":"9323.00000000","q":"0.00166600",
  "b":93225744,"a":93226002,"T":1524828444907,"m":true,"M":true}

  {"e":"trade","E":1524828424904,"s":"XVGBTC","t":14637261,"p":"0.00000787","q":"60850.00000000",
  "b":33387631,"a":33387459,"T":1524828424900,"m":false,"M":true}


  AAPL/CAD
    Where APPL stock is being bought and sold in exchange for Canadian dollars.

  XVGBTC:
    XVG/BTC
    Where XVG is being bought and sold in exchange for BTC.
    If m is true, then the trade is maker of market, who wants to buy XVG with BTC
    XVG_q = q
    BTC_p = p
    
    BTC_Market = XVG_q * BTC_p

  BTCUSDT:
    BTC/USDT
    Where BTC is being bought and sold in exchange for USDT.
    If m(market maker) is true, then the trade is maker of market, who wants to buy BTC with USDT
    BTC_q = q
    USDT_p = p
    
    USDT_Market = BTC_q * USDT_p
    

  XVG/USDT :
    XVG <--- BTC <--- USDT
    Find a market maker who wants to buy XVG with USDT but through BTC.
    i.e Find the market makers who is selling USDT for BTC and BTC for XVG


    **/

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

  case class TradeStreams(e: Option[String],
                          E: Option[String],
                          s: Option[String],
                          t: Option[String],
                          p: Option[Double],
                          q: Option[Double],
                          b: Option[String],
                          a: Option[String],
                          T:Option[Timestamp],
                          m: Option[String],
                          M: Option[String])

//  object TradeStreamsProtocol extends DefaultJsonProtocol {
//    implicit val tradeStreamsFormat = jsonFormat11(TradeStreams)
//  }




}
