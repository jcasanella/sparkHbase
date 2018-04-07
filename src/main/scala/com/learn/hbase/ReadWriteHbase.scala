package com.learn.hbase

import com.learn.hbase.HBaseWriteApp._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.HashTable.HashMapper
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap


object HBaseWriteApp {

  def main(args: Array[String]): Unit = {

    // Create spark session
    val session = SparkSession.builder()
      .appName("StreamApp")
      .master("local[*]")
      .getOrCreate()

    // set up HBase Table configuration
    import collection.JavaConversions._
    val props: Map[String, String] = HashMap(TableInputFormat .INPUT_TABLE -> tableName)
    val hManager = new HbaseConnectionManager(props)


    //val conf = HBaseConfiguration.create()
    //conf.set(TableInputFormat.INPUT_TABLE, tableName)
    //conf.addResource("/etc/hbase/conf/hbase-site.xml")

    //Check if table present
    //val conn = ConnectionFactory.createConnection(conf)
    //val admin  = conn.getAdmin

    //if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
    if (!hManager.isTableAvailable(tableName)) {

      //val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      //tableDesc.addFamily(new HColumnDescriptor(colFam1.getBytes()))
      //tableDesc.addFamily(new HColumnDescriptor(colFam2.getBytes()))
      //tableDesc.addFamily(new HColumnDescriptor(colFam3.getBytes()))

      //admin.createTable(tableDesc)
      val colsFamily = Seq(colFam1, colFam2, colFam3)
      hManager.createTable(tableName, colsFamily)
    }

    //Insert Data into Table
    val sensorTable = conn.getTable(TableName.valueOf(tableName))

    SensorRow.addSensorRow(sensorTable, SensorRow("1", "UK", "Worldpay2", 10, 1000, true))  // Row1
    SensorRow.addSensorRow(sensorTable, SensorRow("2", "UK", "Argos2", 50, 5000, false))  // Row2
    SensorRow.addSensorRow(sensorTable, SensorRow("3", "CAT", "ANC2", 1000, 10000000, true)) // Row3
    SensorRow.addSensorRow(sensorTable, SensorRow("4", "CAT", "Fargo2", 3310, 4561000, true)) // Row4

    sensorTable.close()

    // Read the hBase table
    val hBaseRDD = session.sparkContext.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    // create RDD of result
    val resultRDD = hBaseRDD.map(x => x._2)
    println("Rows readed: " + resultRDD.count())

    // Print values
    val parsedRDD = resultRDD.map(SensorRow.parser)
    parsedRDD.foreach(println)

    // Now using spark sql
    import session.implicits._
    val parseDF = parsedRDD.toDF()
    parseDF.createOrReplaceTempView("sensorView")
    parseDF.printSchema()
    val sensorDF = session.sqlContext.sql("SELECT country, count(*) FROM sensorView GROUP BY country")
    sensorDF.show()

    // Writing using spark
    val confWrite = HBaseConfiguration.create
    confWrite.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    confWrite.addResource("/etc/hbase/conf/hbase-site.xml")

    val jobConf = new Configuration(confWrite)
    jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
    jobConf.set("mapreduce.job.output.value.class", classOf[ImmutableBytesWritable].getName)
    jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    val newData = Array(
      SensorRow("5", "France", "Leucate", 1000, 10000, false),
      SensorRow("6", "Germany", "Wakeboard", 600, 2000, false)
    )
    val newRDD = session.sparkContext.parallelize(newData)
    val hBaseWriteRDD = newRDD.map(SensorRow.convertToPutStats)
    hBaseWriteRDD.saveAsNewAPIHadoopDataset(jobConf)

    // Read hBase table with filter
    val confFilter = HBaseConfiguration.create
    confFilter.set(TableInputFormat.INPUT_TABLE, tableName)
    confFilter.set(TableInputFormat.SCAN_ROW_START, "5")
    confFilter.set(TableInputFormat.SCAN_ROW_STOP, "7")
    confFilter.addResource("/etc/hbase/conf/hbase-site.xml")

    // Read the hBase table
    val hBaseFilterRDD = session.sparkContext.newAPIHadoopRDD(confFilter,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    // create RDD of result
    val resultFilterRDD = hBaseFilterRDD.map(x => x._2)
    println("Rows readed: " + resultFilterRDD.count())

    // Print values
    val parsedFilterRDD = resultFilterRDD.map(SensorRow.parser)
    parsedFilterRDD.foreach(println)

    // close hbase connec
    if (admin != null) {
      admin.close()
    }

    if (conn != null) {
      conn.close()
    }

    // close spark session
    if (session != null) {
      session.close()
    }
  }
}
