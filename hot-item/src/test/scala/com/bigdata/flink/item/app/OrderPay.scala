package com.bigdata.flink.item.app

import java.util

import com.bigdata.flink.item.bean.order.{OrderEvent, OrderResult}
import com.bigdata.flink.item.transform.order.OrderTimeoutAlert
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map
import org.junit.Test

class OrderPay {

  @Test
  def timeoutMonitorByCep(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getResource("/input/OrderLog.csv").getPath
    val orderEventStream = env.readTextFile(path).map{line =>
        val strings = line.split(",")
        OrderEvent(strings(0).toLong,strings(1),strings(3).toLong*1000)
      }.assignAscendingTimestamps(_.eventTime)

    // 定义非紧邻型匹配窗口模式
    val orderPayPattern = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType=="create")
      .followedBy("follow")
      .where(_.eventType=="pay")
      .within(Time.minutes(15))

    // 定义输出标签
    val orderTimeoutOutput = OutputTag[OrderResult]("orderTimeout")

    // 订单流上绑定匹配模式处理器
    val patternStream = CEP.pattern(orderEventStream.keyBy("orderId"), orderPayPattern)

    val completedResult = patternStream.select(orderTimeoutOutput){
      (pattern:Map[String,Iterable[OrderEvent]],timestamp:Long) =>  // 注册超时处理流
      val createdOrder = pattern.get("begin")
      OrderResult(createdOrder.get.iterator.next().orderId,"timeout")
    }{
      (pattern:Map[String,Iterable[OrderEvent]]) => // 注册成功支付处理流
        val payOrder = pattern.get("follow")
        OrderResult(payOrder.get.iterator.next().orderId,"success")
    }

    val timeoutResult = completedResult.getSideOutput(orderTimeoutOutput) // 获取测单输出流

    completedResult.print() // 打印成功订单
    timeoutResult.print() // 打印超时订单

    env.execute("payment timeout monitor by cep")
  }

  @Test
  def timeoutMonitorByProcess(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val timeoutOutputTag = new OutputTag[OrderResult]("orderTimeout")

    val path = getClass.getResource("/input/OrderLog.csv").getPath
    val orderEventStream = env.readTextFile(path)
      .map( data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(3).toLong*1000)
      }).assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    // 自定义一个 process function，进行order的超时检测，输出超时报警信息
    val orderResultStream = orderEventStream.process(new OrderTimeoutAlert(15*60*1000,timeoutOutputTag))

    val timeoutOrderStream = orderResultStream.getSideOutput(timeoutOutputTag)

    orderResultStream.print("success")
    timeoutOrderStream.print("timeout")

    env.execute()
  }


}
