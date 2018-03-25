package com.learn.kafka


import java.net.InetAddress
import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{CommitFailedException, ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConverters._

class ConsumerExample(bootstrap: String, groupId: String, topic: String) {

  require(bootstrap != null && bootstrap.length > 0, "Bootstrap not defined")
  require(groupId != null && groupId.length > 0, "groupId not defined")
  require(topic != null && topic.length > 0, "topic not defined")

  val consumer = {

    val  props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
    props.put(ConsumerConfig.CLIENT_ID_CONFIG, InetAddress.getLocalHost.getHostName) // Should be clientID --- very often all consumers with the same groupID have the same clientId
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000")
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000") // max msg to process in every iteration
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "3000")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    new KafkaConsumer[String, String](props)
  }

  def pollMsgs() = {

    consumer.subscribe(util.Arrays.asList(topic))
    while(true) {
      val records = consumer.poll(1000)
      val iterator = records.iterator()
      while(iterator.hasNext) {
        val consRec = iterator.next()
        println(s"Offset: ${consRec.offset()} msg: ${consRec.value()}")

        try {
          consumer.commitSync()
        } catch {
          case ex: CommitFailedException =>

        }
      }
    }
  }
}

object ConsumerExample extends App {

  if (args.length < 3) {
    System.err.println("Wrong number params")
    System.exit(1)
  }

  val Array(boostrap, groupId, topic) = args
  val kc = new ConsumerExample(boostrap, groupId, topic)
  kc.pollMsgs()
}