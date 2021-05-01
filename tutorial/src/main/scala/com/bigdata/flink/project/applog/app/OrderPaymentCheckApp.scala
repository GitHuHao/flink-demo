package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.OrderEvent
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

/**
 * 订单与支付流水实时对账，检查系统是否存在漏洞
 * 15分钟内完成支付是正常的
 */
object OrderPaymentCheckApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val alertTag = new OutputTag[String]("alert")

    val mainDS = env.readTextFile(CommonSuit.getFile("applog/OrderLog.csv"))
      .map{line =>
        val arr = line.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong*1000L)
      }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
      new SerializableTimestampAssigner[OrderEvent] {
        override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = element.eventTime
      }
    )).keyBy(_.orderId)
      .process(new KeyedProcessFunction[Long,OrderEvent,String] {
        private var orderState:ValueState[OrderEvent] = _
        private var paymentState:ValueState[OrderEvent] = _
        private var timerState:ValueState[Long] = _

        private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        override def open(parameters: Configuration): Unit = {
          orderState = getRuntimeContext.getState(new ValueStateDescriptor("order-state",classOf[OrderEvent]))
          paymentState = getRuntimeContext.getState(new ValueStateDescriptor("payment-state",classOf[OrderEvent]))
          timerState = getRuntimeContext.getState(new ValueStateDescriptor("timer-state",classOf[Long],Long.MinValue))
        }

        override def processElement(i: OrderEvent, context: KeyedProcessFunction[Long, OrderEvent, String]#Context,
                                    collector: Collector[String]): Unit = {
          i.eventType match {
            case "create" => {
              val payment = paymentState.value()
              if(payment==null){
                orderState.update(i)
                val tmstp = i.eventTime + 20*60*1000L
                context.timerService().registerEventTimeTimer(tmstp)// 单侧到达，基于事件时间注册定时任务，20分钟后检查另一条记录是否存在
                timerState.update(tmstp)
              }else{
                payTimeoutCheck(i,payment)
              }
            }
            case "pay" => {
              val order = orderState.value()
              if(order==null){
                paymentState.update(i)
                val tmstp = i.eventTime + 20*60*1000L
                context.timerService().registerEventTimeTimer(tmstp)
                timerState.update(tmstp)
              }else{
                payTimeoutCheck(order,i)
              }
            }
          }

          def payTimeoutCheck(order:OrderEvent,payment:OrderEvent): Unit ={
            val payTime =  sdf.format(new Date(payment.eventTime))
            val orderTime =  sdf.format(new Date(order.eventTime))
            if(payment.eventTime - order.eventTime < 15*60*1000L){
              collector.collect(s"${order.orderId} 订单与支付成功匹配")
            }else{
              context.output(alertTag,s"${order.orderId} 下单：${orderTime}，支付：${payTime}, 下单15分钟后完成支付，请检查支付超时逻辑是否有误")
            }
            if(paymentState.value()!=null){
              paymentState.clear()
            }
            if(orderState.value()!=null){
              orderState.clear()
            }
            if(timerState.value()!=Long.MinValue){
              context.timerService().deleteEventTimeTimer(timerState.value())
              timerState.clear()
            }
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          val payment = paymentState.value()
          if(payment==null){
            out.collect(s"${ctx.getCurrentKey}下单15分钟后，未支付，自动取消订单")
          }else{
            ctx.output(alertTag,s"${ctx.getCurrentKey}有支付后20分钟内未发现订单信息，请检查下单逻辑是否有误")
          }
          if(paymentState.value()!=null){
            paymentState.clear()
          }
          if(orderState.value()!=null){
            orderState.clear()
          }
          if(timerState.value()!=Long.MinValue){
            timerState.clear()
          }
        }
      })

    mainDS.print("ok")
    mainDS.getSideOutput(alertTag).print("alert")

    env.execute(getClass.getSimpleName)
  }

}
