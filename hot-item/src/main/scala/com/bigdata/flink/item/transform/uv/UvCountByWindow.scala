package com.bigdata.flink.item.transform.uv

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.behavior.UserBehavior
import com.bigdata.flink.item.bean.uv.UvCount
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class UvCountByWindow extends AllWindowFunction[UserBehavior,UvCount,TimeWindow]{
  private val sdf = new SimpleDateFormat("yyyy年MM月dd日 HH时")
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    val end = window.getEnd
    val time = sdf.format(new Date(end))
    out.collect(UvCount(time,input.map(_.userId).toSet.size))
  }
}
