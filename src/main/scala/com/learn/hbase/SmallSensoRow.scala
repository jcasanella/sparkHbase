package com.learn.hbase

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes

case class SmallSensorRow(country: String, company: String)

object SmallSensorRow extends Sensor[SmallSensorRow] with Serializable {

  def parser(result: Result): SmallSensorRow = {

    val country = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field1)))  // Country
    val company = Bytes.toString(result.getValue(Bytes.toBytes(colFam1), Bytes.toBytes(colFam1Field2)))  // Company

    SmallSensorRow(country, company)
  }
}
