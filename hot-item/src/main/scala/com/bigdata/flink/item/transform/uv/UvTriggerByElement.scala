package com.bigdata.flink.item.transform.uv

import com.bigdata.flink.item.bean.behavior.UserBehavior
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

class UvTriggerByElement extends Trigger[UserBehavior,TimeWindow]{
  // 元素级别触发
  override def onElement(t: UserBehavior, l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.FIRE_AND_PURGE
  // 窗口级别触发
  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  // 事件级别触发
  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE
  // 清空触发器
  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {}
}
