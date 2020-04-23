package com.bigdata.flink.item.transform.pv

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.behavior.UserBehavior
import com.bigdata.flink.item.bean.pv.PvCount
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class PvCountByWindow extends AllWindowFunction[UserBehavior,PvCount,TimeWindow]{
  private val sdf = new SimpleDateFormat("yyyy年MM月dd日 HH时")
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[PvCount]): Unit = {
    val end = window.getEnd
    val time = sdf.format(new Date(end))
    out.collect(PvCount(time,input.size))
  }
}
