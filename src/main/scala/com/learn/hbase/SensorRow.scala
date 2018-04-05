package com.learn.hbase

import org.apache.hadoop.hbase.client.{Put, Result, Table}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes

case class SensorRow(rowkey: String, country: String, company: String, min: Int, max: Int, status: Boolean)

object NullableFieldSupport {

  implicit class PimpedPut(self: Put) {

    def addNullable(columnFamily: String, columnName: String, value: String): Put = {
      Option(value).fold(self) { notNullValue =>
          self.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))
      }
    }

    def addNullable(columnFamily: String, columnName: String, value: Int): Put = {
      Option(value).fold(self) { notNullValue =>
        self.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))
      }
    }

    def addNullable(columnFamily: String, columnName: String, value: Boolean): Put = {
      Option(value).fold(self) { notNullValue =>
        self.addImmutable(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(value))
      }
    }
  }
}

object SensorRow extends Sensor[SensorRow] with Serializable {

  def parser(result: Result): SensorRow = {

    val rowKey = Bytes.toString(result.getRow()).split(" ")(0)  // Rowkey
    val country = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field1)))  // Country
    val company = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field2)))  // Company
    val minVal = Bytes.toInt(result.getValue(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Field1)))      // Min
    val maxVal = Bytes.toInt(result.getValue(Bytes.toBytes(colFam2), Bytes.toBytes(colFam2Field2)))      // Max
    val status = Bytes.toBoolean(result.getValue(Bytes.toBytes(colFam3), Bytes.toBytes(colFam3Field1)))  // Status

    SensorRow(rowKey, country, company, minVal, maxVal, status)
  }

  import NullableFieldSupport._

  def addSensorRow(sensorTable: Table, row: SensorRow): Unit = {

    var newRow = new Put(row.rowkey.getBytes())
    newRow
      .addNullable(colFam1, colFam1Field1, row.country)
      .addNullable(colFam1, colFam1Field1, row.country)
      .addNullable(colFam1, colFam1Field2, row.company)
      .addNullable(colFam2, colFam2Field1, row.min)
      .addNullable(colFam2, colFam2Field2, row.max)
      .addNullable(colFam3, colFam3Field1, row.status)

    sensorTable.put(newRow)
  }

  def convertToPutStats(row: SensorRow): (ImmutableBytesWritable, Put) = {

    var newRow = new Put(row.rowkey.getBytes())
    newRow
      .addNullable(colFam1, colFam1Field1, row.country)
      .addNullable(colFam1, colFam1Field1, row.country)
      .addNullable(colFam1, colFam1Field2, row.company)
      .addNullable(colFam2, colFam2Field1, row.min)
      .addNullable(colFam2, colFam2Field2, row.max)
      .addNullable(colFam3, colFam3Field1, row.status)

    (new ImmutableBytesWritable, newRow)
  }
}
