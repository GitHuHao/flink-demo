package com.bigdata.flink.item.transform.market

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.market.MarketingCountView
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MarketingCountByBehavior  extends ProcessWindowFunction[(String,Long),MarketingCountView,String,TimeWindow]{
  private val sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[MarketingCountView]): Unit = {
    val startTime = sdf.format(new Date(context.window.getStart))
    val endTime = sdf.format(new Date(context.window.getEnd))
    val count = elements.size
    out.collect(MarketingCountView(startTime,endTime,"total",key,count))
  }
}
