package com.binance

import org.rogach.scallop.ScallopConf

class RuntimeConfig(arguments: Seq[String]) extends ScallopConf(arguments)
  with Serializable {

  val batchTimeInSeconds = opt[Long](default = Option(10),
    descr = """Streaming batch time in seconds""")

  version("Blockchain Streaming Runner 0.0.1")
  banner(
    """
      |Currently this program supports exploratory analysis on Binance streams using Spark Streaming framework
      |
      |Options:
    """.stripMargin)

  verify()
}