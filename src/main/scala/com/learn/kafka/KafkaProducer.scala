package com.learn.kafka

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.util.HashMap

object KafkaProducer extends App {

  if (args.length < 4) {
    System.err.println("Usage: KafkaProducer <brokerList> <topic> <msgPerSec> <wordsMsg>")
    System.exit(1)
  }

  val Array(brokerList, topic, msgSec, wordsMsg) = args

  val props = new HashMap[String, Object]()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  while(true){
    val str = (1 to msgSec.toInt).foreach { msgNum =>
      val str = (scala.util.Random.nextInt(msgNum).toString).mkString(" ")
      val msg = new ProducerRecord[String, String](topic, null, s"Msg ${str}")
      producer.send(msg)
    }

    Thread.sleep(1000)
  }


}
