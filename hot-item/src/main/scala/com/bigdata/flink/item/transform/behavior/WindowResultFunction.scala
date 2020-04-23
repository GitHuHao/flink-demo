package com.bigdata.flink.item.transform.behavior

import com.bigdata.flink.item.bean.behavior.ItemViewCount
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// 窗口函数只有在触发窗口滑动时只需输出
class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId:Long = key.getField(0)
    val count = input.iterator.next()
    // window.getEnd 窗口结束时间戳
    out.collect(ItemViewCount(itemId,window.getEnd,count))
  }
}
