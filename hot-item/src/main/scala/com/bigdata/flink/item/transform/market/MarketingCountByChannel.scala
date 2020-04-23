package com.bigdata.flink.item.transform.market

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.market.MarketingCountView
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class MarketingCountByChannel extends ProcessWindowFunction[((String,String),Long),MarketingCountView,(String,String),TimeWindow]{

  private val sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")

  override def process(key: (String, String), context: Context,
                       elements: Iterable[((String, String), Long)], out: Collector[MarketingCountView]): Unit = {

    val endTmstp = context.window.getEnd
    val startTmstp = context.window.getStart

    val (channel,behavior) = key
    val count = elements.size

    val endDate = sdf.format(new Date(endTmstp))
    val startDate = sdf.format(new Date(startTmstp))

    out.collect(MarketingCountView(startDate,endDate,channel,behavior,count))

  }
}
