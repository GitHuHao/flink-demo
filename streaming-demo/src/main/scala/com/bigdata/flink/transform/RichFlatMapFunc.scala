package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

class RichFlatMapFunc extends RichFlatMapFunction[String, String] {
  var startTime: Long = _

  override def open(parameters: Configuration): Unit = {
    startTime = System.currentTimeMillis()
  }

  override def flatMap(in: String, collector: Collector[String]): Unit = {
    // 每条记录的处理时间
    val str: String = in + "处理时间:" + System.currentTimeMillis()
    println(s"开始时间:$startTime, 当前数据$str")
    in.split("\\W+").foreach(collector.collect(_))
  }

  override def close(): Unit = {}
}

class MyFlatMap extends RichFlatMapFunction[Int, (Int, Int)] {
  var subTaskIndex = 0

  override def open(configuration: Configuration): Unit = {
    subTaskIndex = getRuntimeContext.getIndexOfThisSubtask
    // 以下可以做一些初始化工作，例如建立一个和HDFS的连接
  }

  override def flatMap(in: Int, out: Collector[(Int, Int)]): Unit = {
    if (in % 2 == subTaskIndex) {
      out.collect((subTaskIndex, in))
    }
  }

  override def close(): Unit = {
    // 以下做一些清理工作，例如断开和HDFS的连接。
  }
}