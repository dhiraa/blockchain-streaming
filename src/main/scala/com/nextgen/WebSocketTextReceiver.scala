//package com.nextgen
//
//import org.apache.spark.storage.StorageLevel
//import org.jfarcand.wcs.{TextListener, WebSocket}
//
//class WebSocketTextReceiver(endpoint: String, outgoingMessage: String = "", storageLevel: StorageLevel = StorageLevel.OFF_HEAP)
//  extends WebSocketReceiver[String](endpoint, storageLevel) {
//
//  override def onStart(): Unit = {
//    super.onStart()
//    if (!outgoingMessage.isEmpty) {
//      websocket.get.send(outgoingMessage)
//    }
//  }
//
//  override def initWebSocket(endpoint: String): WebSocket = WebSocket().open(endpoint).listener(new TextListener() {
//    override def onMessage(message: String): Unit = store(message)
//  })
//}