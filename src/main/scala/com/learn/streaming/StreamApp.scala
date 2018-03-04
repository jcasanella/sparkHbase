package com.learn.streaming

import com.learn.streaming.HBaseWriteApp._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.sql.SparkSession

case class SmallSensorRow(country: String, company: String)

object SmallSensorRow {


  def parseSensorRow(result: Result): SmallSensorRow = {

    val country = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field1)))  // Country
    val company = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field2)))  // Company

    SmallSensorRow(country, company)
  }
}

case class SensorRow(rowkey: String, country: String, company: String, min: Int, max: Int, status: Boolean)

object SensorRow {

  def parseSensorRow(result: Result): SensorRow = {

    val rowKey = Bytes.toString(result.getRow()).split(" ")(0)  // Rowkey
    val country = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field1)))  // Country
    val company = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field2)))  // Company
    val minVal = Bytes.toInt(result.getValue(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Field1)))      // Min
    val maxVal = Bytes.toInt(result.getValue(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Field2)))      // Max
    val status = Bytes.toBoolean(result.getValue(Bytes.toBytes(colFam3), Bytes.toBytes(colFam3Field1)))  // Status

    SensorRow(rowKey, country, company, minVal, maxVal, status)
  }

  def addSensorRow(sensorTable: Table, row: SensorRow): Unit = {

    var newRow = new Put(row.rowkey.getBytes())
    newRow.addImmutable(colFam1.getBytes(), colFam1Field1.getBytes(), row.country.getBytes())
    newRow.addImmutable(colFam1.getBytes(), colFam1Field2.getBytes(), row.company.getBytes())
    newRow.addImmutable(colFam2.getBytes(), colFam2Field1.getBytes(), Bytes.toBytes(row.min))
    newRow.addImmutable(colFam2.getBytes(), colFam2Field2.getBytes(), Bytes.toBytes(row.max))
    newRow.addImmutable(colFam3.getBytes(), colFam3Field1.getBytes(), Bytes.toBytes(row.status))

    sensorTable.put(newRow)
  }

  def convertToPutStats(row: SensorRow): (ImmutableBytesWritable, Put) = {

    var newRow = new Put(Bytes.toBytes(row.rowkey))
    newRow.addImmutable(colFam1.getBytes(), colFam1Field1.getBytes(), row.country.getBytes())
    newRow.addImmutable(colFam1.getBytes(), colFam1Field2.getBytes(), row.country.getBytes())
    newRow.addImmutable(colFam2.getBytes(), colFam2Field1.getBytes(), Bytes.toBytes(row.min))
    newRow.addImmutable(colFam2.getBytes(), colFam2Field2.getBytes(), Bytes.toBytes(row.max))
    newRow.addImmutable(colFam3.getBytes(), colFam3Field1.getBytes(), Bytes.toBytes(row.status))

    (new ImmutableBytesWritable, newRow)
  }
}

object HBaseWriteApp {

  // hBase attributes - Name and col family
  val tableName = "sensor"

  val colFam1 = "data"
  val colFam1Field1 = "country"
  val colFam1Field2 = "company"

  val colFam2 = "alert"
  val colFam2Field1 = "min"
  val colFam2Field2 = "max"

  val colFam3 = "stats"
  val colFam3Field1 = "status"

  def main(args: Array[String]): Unit = {

    // Create spark session
    val session = SparkSession.builder()
      .appName("StreamApp")
      .master("local[*]")
      .getOrCreate()

    // set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
   // conf.set("zookeeper.znode.parent ","/hbase-unsecure")
    conf.addResource("/etc/hbase/conf/hbase-site.xml")

    //Check if table present
    val conn = ConnectionFactory.createConnection(conf)
    val admin  = conn.getAdmin

    if (!admin.isTableAvailable(TableName.valueOf(tableName))) {

      val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
      tableDesc.addFamily(new HColumnDescriptor(colFam1.getBytes()))
      tableDesc.addFamily(new HColumnDescriptor(colFam2.getBytes()))
      tableDesc.addFamily(new HColumnDescriptor(colFam3.getBytes()))

      admin.createTable(tableDesc)
    }

    //Insert Data into Table
    val sensorTable = conn.getTable(TableName.valueOf(tableName))

    // row 1
    SensorRow.addSensorRow(sensorTable, SensorRow("1", "UK", "Worldpay2", 10, 1000, true))
    // row 2
    SensorRow.addSensorRow(sensorTable, SensorRow("2", "UK", "Argos2", 50, 5000, false))
    // row 3
    SensorRow.addSensorRow(sensorTable, SensorRow("3", "CAT", "ANC2", 1000, 10000000, true))
    // row 4
    SensorRow.addSensorRow(sensorTable, SensorRow("4", "CAT", "Fargo2", 3310, 4561000, true))

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
    val parsedRDD = resultRDD.map(SensorRow.parseSensorRow)
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
    val parsedFilterRDD = resultFilterRDD.map(SensorRow.parseSensorRow)
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
