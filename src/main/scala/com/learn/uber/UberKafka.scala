package com.learn.uber

import com.learn.ParamsUtils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import scala.util.Try

case class Uber(dt: String, lat: Double, lon: Double, base: String)

class UberKafka(bootstrap: Try[String], groupId: Try[String], clientId: Try[String], topic: Try[String]) extends Serializable {

  require(bootstrap.isSuccess, "Bootstrap not valid")
  require(groupId.isSuccess, "GroupId not valid")
  require(clientId.isSuccess, "ClientId not valid")
  require(topic.isSuccess, "Topic not valid")

  private def setUpProperties(): Map[String, String] = {

    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrap.get,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId.get,
      ConsumerConfig.CLIENT_ID_CONFIG -> clientId.get,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> "8192"
    )

    kafkaParams
  }

  def getMessages(implicit ssc: StreamingContext) = {

    val kafkaParams = setUpProperties()

    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Array(topic.get), kafkaParams)
    )
  }
}

object UberKafka extends App {

  private def parseUber(str: String): Uber = {

    val p = str.split(",")
    Uber(p(0), p(1).toDouble, p(2).toDouble, p(3))
  }

  val session_stream = UberSpark()
  implicit val ssc = session_stream._2

  val Array(bootstrap, groupId, clientId, topic) = args.map(ParamsUtils.validateAttribute(_))
  val uber = new UberKafka(bootstrap, groupId, clientId, topic)

  // Read msg
  val msg = uber.getMessages

  // Process msg
  val valuesDStream = msg.map(_.value())
  valuesDStream.foreachRDD(rdd => {
    if (!rdd.isEmpty()) {
      val count = rdd.count()
      println(s"Number of rows: $count")

      // Create sqlContext
      val spark = SparkSession.builder
        .config(rdd.sparkContext.getConf)
        .getOrCreate()

      import spark.implicits._

      val df = rdd.map(parseUber(_)).toDF()
      df.show()
    }
  })

  // Start and stop spark
  ssc.start()
  ssc.awaitTermination()
}

