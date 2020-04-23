package com.bigdata.flink.item.transform.url

import java.sql.Timestamp

import com.bigdata.flink.item.bean.url.UrlViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

class TopNHotUrls(topSize:Int) extends KeyedProcessFunction[Tuple,UrlViewCount,String]{

  private var urlState:ListState[UrlViewCount] = _

  override def open(parameters: Configuration): Unit = {
    // 注册 itemState
    super.open(parameters)
    val urlsStateDesc = new ListStateDescriptor[UrlViewCount]("urlState-state", classOf[UrlViewCount])
    urlState = getRuntimeContext.getListState(urlsStateDesc)
  }

  override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Tuple, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    // 收集元素并注册触发器
    urlState.add(i)
    context.timerService().registerEventTimeTimer(i.windowEnd+1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 触发，取 top N
    val buffer:ListBuffer[UrlViewCount] = ListBuffer()
    for(urlView <- urlState.get()){
      buffer += urlView
    }
    urlState.clear() // 清空

    val sortedUrlViews = buffer.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val builder = new StringBuilder
    builder.append(s"============================\n时间: ${new Timestamp(timestamp -1)}\n")
    for(i <- sortedUrlViews.indices){
      val currentUrlView = sortedUrlViews(i)
      builder.append(s"No ${i+1}:\tURL=${currentUrlView.url}\t流量=${currentUrlView.count}\n")
    }
    builder.append("============================\n\n")
    Thread.sleep(1000)

    out.collect(builder.toString())
  }


}
