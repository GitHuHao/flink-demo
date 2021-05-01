package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

class RichMapFunc extends RichMapFunction[String, String] {
  var startTime: Long = _

  override def open(parameters: Configuration): Unit = {
    startTime = System.currentTimeMillis()
  }

  override def map(in: String): String = {
    // 每条记录的处理时间
    val str: String = in + "处理时间:" + System.currentTimeMillis()
    println(s"开始时间:$startTime, 当前数据$str")
    in.toUpperCase
  }

  override def close(): Unit = {}
}