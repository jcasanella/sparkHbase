package com.learn.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.util.Bytes

trait HBaseConnectionManager {

  self: Serializable =>
  def connection: Connection
}

class HbaseConnectionManager(configProps: Map[String, String]) extends HBaseConnectionManager with Serializable {

  @transient
  lazy val config = {
    val conf = configProps.foldLeft(HBaseConfiguration.create()) {
      case (conf, entry) => conf.set(entry._1, entry._2)
        conf
    }
    conf.addResource("/etc/hbase/conf/hbase-site.xml")
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

  @transient
  lazy val admin = connection.getAdmin

  def cleanup() = {

    if (admin != null)
      admin.close()

    if (!connection.isClosed)
      connection.close()
  }

  def isTableAvailable(tableName: String): Boolean = admin.isTableAvailable(TableName.valueOf(tableName))

  def createTable(tableName: String, colsFamily: Seq[String]) = {

    val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
    colsFamily.map(col => tableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes(col))))
    admin.createTable(tableDesc)
  }
}

