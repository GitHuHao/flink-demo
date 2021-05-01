package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.{RichFilterFunction}
import org.apache.flink.configuration.Configuration

class RichFilterFunc extends RichFilterFunction[String, String] {
  var startTime: Long = _

  override def open(parameters: Configuration): Unit = {
    startTime = System.currentTimeMillis()
  }

  override def filter(in: String): Boolean = {
    // 每条记录的处理时间
    val str: String = in + "处理时间:" + System.currentTimeMillis()
    println(s"开始时间:$startTime, 当前数据$str")
    in.contains("i")
  }

  override def close(): Unit = {}
}