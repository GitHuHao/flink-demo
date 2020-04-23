package com.bigdata.flink.item.transform.url

import com.bigdata.flink.item.bean.url.UrlViewCount
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 窗口函数只有在触发窗口滑动时只需输出
class WindowResultFunction extends WindowFunction[Long,UrlViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    val url:String = key.getField(0)
    val count = input.iterator.next()
    // window.getEnd 窗口结束时间戳
    out.collect(UrlViewCount(url,window.getEnd,count))
  }
}
