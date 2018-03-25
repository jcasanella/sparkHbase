package com.learn.streaming.parquet

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}


case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)
object Sensor extends Serializable {

  // function to parse line of sensor data into Sensor class
  def parser(str: String): Sensor = {

    val p = str.split(",")
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
  }
}

/**
  * How to run -
  * a) Run the process: See script.sh
  * b) Copy the csv from data to hdfs /user/cloudera/data
  */
object StreamParquetApp extends App {

  // Create session
  val session = SparkSession.builder
      .appName("StreamParquetApp")
      .getOrCreate()

  println("Starting App!!!")

  val ssc = new StreamingContext(session.sparkContext, Seconds(2))

  // Creates DStream from file system
  val dstream = ssc.textFileStream("hdfs://quickstart.cloudera:8020/user/cloudera/data/")
  //val words = dstream.flatMap(stream => stream.split(","))
  //val wordCounts = words.map(word => (word, 1)).reduceByKey(_ + _)
  //wordCounts.print()

  import session.implicits._
  dstream.foreachRDD(rdd => {
    val sensorRDD = rdd.map(r => Sensor.parser(r))
    val df = sensorRDD.toDF
    //df.take(5).foreach(println)
    df.write.mode("append").parquet("hdfs://quickstart.cloudera:8020/user/cloudera/data_parquet")
  })

  // Start process
  ssc.start()
  ssc.awaitTermination()

  if (session != null) {
    session.close()
  }
}
