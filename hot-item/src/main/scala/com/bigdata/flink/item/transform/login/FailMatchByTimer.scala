package com.bigdata.flink.item.transform.login

import java.text.SimpleDateFormat

import com.bigdata.flink.item.bean.login.{LoginEvent, Warning}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

class FailMatchByTimer(limit: Long) extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  private lazy val failedState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("failed state", classOf[Long]))

  private lazy val sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")

  override def processElement(loginEvent: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    val failedAt = failedState.value()
    if (loginEvent.eventType == "fail") {
      if (failedAt != null) {
        if (loginEvent.eventTime < failedAt + limit) {
          val msg = s"login fail twice in ${limit} ms"
          collector.collect(Warning(loginEvent.userId, sdf.format(failedAt), sdf.format(loginEvent.eventTime), msg))
        }
        context.timerService().deleteEventTimeTimer(loginEvent.eventTime)
      }
      failedState.update(loginEvent.eventTime)
      context.timerService().registerEventTimeTimer(loginEvent.eventTime + limit)
    } else {
      if (failedAt != null) {
        failedState.clear()
        context.timerService().deleteEventTimeTimer(loginEvent.eventTime)
      }
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    failedState.clear()
  }

}
