package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.OrderEvent
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import java.time.Duration
import java.util

/**
 * 订单与支付直接超过15分钟，就超时报警
 */

object OrderPaymentCheckByCEPApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val alertTag = new OutputTag[String]("alert")
    val keyedDS = env.readTextFile(CommonSuit.getFile("applog/OrderLog.csv"))
      .map { line =>
        val arr = line.split(",")
        OrderEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong * 1000L)
      }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
      new SerializableTimestampAssigner[OrderEvent] {
        override def extractTimestamp(element: OrderEvent, recordTimestamp: Long): Long = element.eventTime
      }
    )).keyBy(_.orderId)

    val pattern = Pattern.begin("create", AfterMatchSkipStrategy.skipPastLastEvent()).where(new SimpleCondition[OrderEvent]() {
      override def filter(t: OrderEvent): Boolean = t.eventType.equals("create")
    }).optional.next("pay").where(new SimpleCondition[OrderEvent]() {
      override def filter(t: OrderEvent): Boolean = t.eventType.equals("pay")
    }).within(Time.minutes(15))

    val mainDS = CEP.pattern(keyedDS, pattern).select(
      alertTag,
      new PatternTimeoutFunction[OrderEvent, String]() {
        override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): String = map.toString
      },
      new PatternSelectFunction[OrderEvent, String]() {
        override def select(map: util.Map[String, util.List[OrderEvent]]): String= map.toString
      })

    mainDS.print("ok")
    mainDS.getSideOutput(alertTag).print("alert")

    env.execute(getClass.getSimpleName)
  }


}
