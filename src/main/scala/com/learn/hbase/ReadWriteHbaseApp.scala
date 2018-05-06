package com.learn.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

import scala.collection.immutable.HashMap


object ReadWriteHbaseApp {

  def main(args: Array[String]): Unit = {

    // Create spark session
    val session = SparkSession.builder()
      .appName("StreamApp")
      .master("local[*]")
      .getOrCreate()

    // set up HBase Table configuration
    import collection.JavaConversions._
    val props: Map[String, String] = HashMap(TableInputFormat.INPUT_TABLE -> tableName)
    val hManager = new HbaseConnectionManager(props)

    //Check if table present
    if (!hManager.isTableAvailable(tableName)) {

      //admin.createTable(tableDesc)
      val colsFamily = Seq(colFam1, colFam2, colFam3)
      hManager.createTable(tableName, colsFamily)
    }

    //Insert Data into Table
    val sensorTable: Table = hManager.connection.getTable(TableName.valueOf(tableName))
    val dadesSensor = Seq(
      SensorRow("1", "UK", "Worldpay2", 10, 1000, status = true),
      SensorRow("2", "UK", "Argos2", 50, 5000, status = false),
      SensorRow("3", "CAT", "ANC2", 1000, 10000000, status = true),
      SensorRow("4", "CAT", "Fargo2", 3310, 4561000, status = true)
    )
    val fInsert = (hTable: Table, row: SensorRow) => SensorRow.addSensorRow(hTable, row)
    dadesSensor.foreach(fInsert(sensorTable, _))
    sensorTable.close()

    // Read the hBase table
    val hBaseRDD = session.sparkContext.newAPIHadoopRDD(hManager.config,
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
    val propsWrite: Map[String, String] = HashMap(TableOutputFormat.OUTPUT_TABLE -> tableName)
    val hManagerWrite = new HbaseConnectionManager(propsWrite)

    val jobConf = new Configuration(hManagerWrite.config)
    jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
    jobConf.set("mapreduce.job.output.value.class", classOf[ImmutableBytesWritable].getName)
    jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)

    val newData = Seq(
      SensorRow("5", "France", "Leucate", 1000, 10000, status = false),
      SensorRow("6", "Germany", "Wakeboard", 600, 2000, status = false)
    )
    val newRDD = session.sparkContext.parallelize(newData)
    val hBaseWriteRDD = newRDD.map(SensorRow.convertToPutStats)
    hBaseWriteRDD.saveAsNewAPIHadoopDataset(jobConf)

    // Read hBase table with filter
    val propsRead: Map[String, String] = HashMap(
      TableInputFormat.INPUT_TABLE -> tableName,
      TableInputFormat.SCAN_ROW_START -> "5",
      TableInputFormat.SCAN_ROW_STOP -> "7"
    )
    val hManagerRead = new HbaseConnectionManager(propsRead)

    // Read the hBase table
    val hBaseFilterRDD = session.sparkContext.newAPIHadoopRDD(hManagerRead.config,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])

    // create RDD of result
    val resultFilterRDD = hBaseFilterRDD.map(x => x._2)
    println("Rows readed: " + resultFilterRDD.count())

    // Print values
    val parsedFilterRDD = resultFilterRDD.map(SensorRow.parser)
    parsedFilterRDD.foreach(println)

    // close spark session
    if (session != null) {
      session.close()
    }
  }
}
