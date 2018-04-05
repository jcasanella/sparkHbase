package com.learn

package object hbase {

  val tableName = "sensor"

  val colFam1 = "data"
  val colFam1Field1 = "country"
  val colFam1Field2 = "company"

  val colFam2 = "alert"
  val colFam2Field1 = "min"
  val colFam2Field2 = "max"

  val colFam3 = "stats"
  val colFam3Field1 = "status"
}
