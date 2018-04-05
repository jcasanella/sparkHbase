package com.learn.hbase

import org.apache.hadoop.hbase.client.Result

trait Sensor[T] {

  self: Serializable =>

  def parser(result: Result): T
}
