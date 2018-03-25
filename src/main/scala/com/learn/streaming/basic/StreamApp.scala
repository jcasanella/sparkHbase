package com.learn.streaming.basic

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.tools.nsc.interpreter.session

object StreamApp extends App {

  val session = SparkSession.builder()
      .appName("StreamApp")
      .master("local[*]")
      .getOrCreate()

  // Create stream context
  val ssc = new StreamingContext(session.sparkContext, Seconds(1))

  // Read and parse
  // From another window - run "nc -l localhot 3333"
  val lines = ssc.socketTextStream("localhost", 3333)
  val words = lines.flatMap(_.split(" "))
  val pairs = words.map((_, 1))
  val wordCounts = pairs.reduceByKey(_ + _)

  wordCounts.print

  ssc.start()
  ssc.awaitTermination()
}
