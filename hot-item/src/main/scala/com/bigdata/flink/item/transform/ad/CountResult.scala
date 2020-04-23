package com.bigdata.flink.item.transform.ad

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.ad.CountByProvince
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class CountResult extends WindowFunction[Long,CountByProvince,String,TimeWindow]{

  private val sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[CountByProvince]): Unit = {
    val windowEnd = sdf.format(new Date(window.getEnd))
    out.collect(CountByProvince(windowEnd,key,input.iterator.next()))
  }
}
