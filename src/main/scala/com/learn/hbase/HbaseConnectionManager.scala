package com.learn.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

trait HBaseConnectionManager {

  self: Serializable =>
  def connection: Connection
}

class HbaseConnectionManager(configProps: Map[String, String]) extends HBaseConnectionManager with Serializable {

  @transient
  private lazy val config = configProps.foldLeft(HBaseConfiguration.create()) {
    case (conf, entry) => conf.set(entry._1, entry._2)
    conf
  }

  @transient
  override lazy val connection: Connection = {
    val connection = ConnectionFactory.createConnection(config)
    sys.ShutdownHookThread {
      cleanup
    }

    connection
  }

  def cleanup() = {

    if (!connection.isClosed)
      connection.close()
  }
}

