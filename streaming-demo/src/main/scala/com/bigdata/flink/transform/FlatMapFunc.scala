package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.util.Collector

// 扁平化，一条输入，多条输出
class FlatMapFunc extends FlatMapFunction[String,String]{
  override def flatMap(t: String, collector: Collector[String]): Unit = {
    t.split("\\W+").foreach(collector.collect(_))
  }
}
