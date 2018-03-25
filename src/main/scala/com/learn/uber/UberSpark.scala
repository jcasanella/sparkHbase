package com.learn.uber

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

class UberSpark extends Serializable{

  val session_stream = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("UberData Kafka")
    val ssc = new StreamingContext(conf, Seconds(2))
    (conf, ssc)
  }
}

object UberSpark {

  def apply(): (SparkConf, StreamingContext) = {
    val us = new UberSpark
    us.session_stream
  }
}
