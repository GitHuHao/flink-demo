package com.bigdata.flink.item.transform.order

import com.bigdata.flink.item.bean.order.{OrderEvent, OrderResult}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.{Collector, OutputTag}

class OrderTimeoutAlert(timeout:Long,timeoutOutputTag:OutputTag[OrderResult]) extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 记录是否支付完毕
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  // 数据没到，需要等待，记录定时器触发时间
  lazy val timerState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-state",classOf[Long]))

  /**
   * 数据链完整的，在processElement 中处理
   * @param event
   * @param ctx
   * @param out
   */
  override def processElement(event: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {
    // 先获取 支付状态 和 是否注册就定时器
    val isPayed = isPayedState.value()
    var timerAt = timerState.value()

    // 已经到的数据将在如下逻辑里面处理
    // 1.create -> pay 注册定时器检测是否超时（后续在已经接收数据中如果发现了pay，就删除此定时器，否则后等待在onTimer中判断超时与否，pay 存在且超时的逻辑在processElement中完成，pay超时且不存在的逻辑在 onTimer中完成）
    // 2.pay -> create 注册定时器检测是否具有创建日志（后续在已经接收数据中如果发现create，就删除此定时器，否则就在onTimer中判断是否存在create log）

    if(event.eventType=="create"){
      if(isPayed){
        // 2-2.pay -> create 注册定时器检测是否具有创建日志（后续在已经接收数据中如果发现create，就删除此定时器，否则就在onTimer中判断是否存在create log）
        if(timerAt>event.eventTime+timeout){
          ctx.output(timeoutOutputTag,OrderResult(event.orderId,"payed but already timeout"))
        }else{
          out.collect(OrderResult(event.orderId,"payed successfully"))
        }
        ctx.timerService().deleteEventTimeTimer(timerAt) // 删除 5-1 中注册的 查找 create log 是否具备的定时器
        isPayedState.clear() // 清空状态
        timerState.clear()
      }else{
        // 1-1.create -> pay 注册定时器检测是否超时（后续在已经接收数据中如果发现了pay，就删除此定时器，否则后等待在onTimer中判断超时与否）
        timerAt = event.eventTime + timeout
        ctx.timerService().registerEventTimeTimer(event.eventTime + timeout)
        timerState.update(timerAt)
      }
    }else if(event.eventType=="pay") {
      if (timerAt > 0) {
        // 1-2.create -> pay 注册定时器检测是否超时（后续在已经接收数据中如果发现了pay，就删除此定时器，否则后等待在onTimer中判断超时与否）
        if (event.eventTime < timerAt) {
          out.collect(OrderResult(event.orderId, "payed successfully"))
        } else {
          ctx.output(timeoutOutputTag, OrderResult(event.orderId, "payed but already timeout"))
        }
        ctx.timerService().deleteEventTimeTimer(timerAt) // 删除 4-1 中注册的 查找 pay 是否超时的定时器
        isPayedState.clear()
        timerState.clear()
      } else {
        // 2-1.pay -> create 注册定时器检测是否具有创建日志（后续在已经接收数据中如果发现create，就删除此定时器，否则就在onTimer中判断是否存在create log）
        isPayedState.update(true)
        ctx.timerService().registerEventTimeTimer(event.eventTime)
        timerState.update(event.eventTime)
      }
    }
  }

  /**
   * 数据链不完整的，在onTimer 中处理
   * @param timestamp
   * @param ctx
   * @param out
   */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {
     val isPayed = isPayedState.value()
     if(isPayed){
       // 2-3.pay -> create 注册定时器检测是否具有创建日志（后续在已经接收数据中如果发现create，就删除此定时器，否则就在onTimer中判断是否存在create log）
       ctx.output(timeoutOutputTag,OrderResult(ctx.getCurrentKey,"already payed but not found create log"))
     }else{
       // 1-3.create -> pay 注册定时器检测是否超时（后续在已经接收数据中如果发现了pay，就删除此定时器，否则后等待在onTimer中判断超时与否）
       ctx.output(timeoutOutputTag,OrderResult(ctx.getCurrentKey,"order timeout"))
     }
    isPayedState.clear()
    timerState.clear()
  }
}