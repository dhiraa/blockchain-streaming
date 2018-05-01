import com.binance.websocket.data.Schema.TradeStreams
import com.binance.websocket.data.Schema

val str =
  """{"e":"trade","E":1525099818890,"s":"BTCUSDT","t":39719091,"p":"9276.84000000","q":"0.00001400","b":94951880,"a":94951891,"T":1525099818890,"m":true,"M":true}""".stripMargin


Schema.parseTradeStreams(str)
