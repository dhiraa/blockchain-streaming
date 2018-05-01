name := "blockchain-streaming"

version := "0.1"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

libraryDependencies ++= Seq(
//  "org.slf4j" % "slf4j-api" % "1.7.5",
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
  "io.spray" %% "spray-json" % "1.3.2",

  "com.typesafe.akka" %% "akka-http" % "10.0.11",
  "com.typesafe.akka" %% "akka-http-testkit" % "10.0.11" % Test,

  "com.typesafe.play" %% "play-ahc-ws-standalone" % "1.1.3",
  "com.typesafe.akka" %% "akka-actor" % "2.4.0",
  "org.twitter4j" %"twitter4j-stream" %"4.0.3",
  "com.typesafe.play" %% "play-json" % "2.4.6",

  "org.rogach" %% "scallop" % "2.1.1",

  "org.scala-lang" % "scala-actors" % "2.11.7",
  "com.typesafe.akka" %% "akka-stream" % "2.4.2",
  "com.typesafe.akka" %% "akka-stream-kafka" % "0.19",
  "com.softwaremill.reactivekafka" %% "reactive-kafka-core" % "0.10.0"
).map(_.exclude ("org.slf4j", "log4j-over-slf4j")) //http://www.slf4j.org/codes.html#log4jDelegationLoop

//libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7"
//// https://mvnrepository.com/artifact/net.liftweb/lift-json
//libraryDependencies += "net.liftweb" %% "lift-json" % "3.2.0"
