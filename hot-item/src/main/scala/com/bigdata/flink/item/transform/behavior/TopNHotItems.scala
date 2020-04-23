package com.bigdata.flink.item.transform.behavior

import java.sql.Timestamp

import com.bigdata.flink.item.bean.behavior.ItemViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class TopNHotItems(topSize:Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String]{

  private var itemState:ListState[ItemViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 注册 itemState
    super.open(parameters)
    val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
    itemState = getRuntimeContext.getListState(itemsStateDesc)
  }

  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 收集元素并注册触发器
    itemState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 触发，取 top N
    val buffer:ListBuffer[ItemViewCount] = ListBuffer()
    for(item <- itemState.get()){
      buffer += item
    }
    itemState.clear() // 清空

    val sortedItems = buffer.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val builder = new StringBuilder
    builder.append(s"============================\n时间: ${new Timestamp(timestamp -1)}\n")
    for(i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      builder.append(s"No ${i+1}:\t商品ID=${currentItem.itemId}\t浏览量=${currentItem.count}\n")
    }
    builder.append("============================\n\n")
    Thread.sleep(1000)

    out.collect(builder.toString())
  }


}
