package com.bigdata.flink.item.transform.black

import com.bigdata.flink.item.bean.ad.AdClickLog
import com.bigdata.flink.item.bean.black.BlackListWarning
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

class FilterBlackListUser(maxCount:Long,blackListOutputTag:OutputTag[BlackListWarning]) extends KeyedProcessFunction[(Long,Long),AdClickLog,AdClickLog]{

  // 保存当前用户对当前广告点击量
  private lazy val countState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
  // 标记当前(用户、广告)作为key是否是第一次发送黑名单
  private lazy val firstSend:ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("firstSend-state",classOf[Boolean]))
  // 标记当前（用户、广告）需要情况状态时间戳
  private lazy val resetTime:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("resetTime-state",classOf[Long]))

  override def processElement(log: AdClickLog, context: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#Context, collector: Collector[AdClickLog]): Unit = {
    // 当前(用户、广告)点击数
    val currentCount = countState.value()
    // 如果是首次点击，则注册当前用户，广告 点击数状态在00:00 清空
    if(currentCount==0l){
      val dailyMills = 24*3600*1000
      val triggerAt = (context.timerService().currentProcessingTime()/dailyMills + 1) *  dailyMills
      resetTime.update(triggerAt)
      context.timerService().registerProcessingTimeTimer(triggerAt)
    }

    // (用户、广告)点击数 达到上限，并且之前没有发送过，则加入黑名单，并使用侧端输出发送报警
    if(currentCount>maxCount){ // 达到上限
      if(!firstSend.value()){ // 今天尚未发送过报警
        firstSend.update(true)
        val msg = s"Click over ${maxCount} times today."
        context.output(blackListOutputTag,BlackListWarning(log.userId,log.adId,msg)) // 侧端输出
      }
    }else{
      countState.update(currentCount + 1) // 未达到上限，继续累加
      collector.collect(log)
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickLog, AdClickLog]#OnTimerContext, out: Collector[AdClickLog]): Unit = {
    // 当前(用户、广告) 点击计数器清空
   if(timestamp == resetTime.value()){
     firstSend.clear()
     countState.clear()
   }
  }

}
