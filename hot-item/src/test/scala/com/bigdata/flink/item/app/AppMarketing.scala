package com.bigdata.flink.item.app

import com.bigdata.flink.item.transform.market.{MarketingCountByBehavior, MarketingCountByChannel, SimulatedEventSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class AppMarketing {

  /**
   * 基于 渠道 动作统计
   */
  @Test
  def byChannel(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val stream = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)

    stream.filter(_.behavior!="UNINSTALL")
      .map(log=>((log.behavior,log.channel),1l))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(1))
      .process(new MarketingCountByChannel)
      .print()

    env.execute("App Marketing by Channel")
  }

  /**
   * 基于动作统计
   */
  @Test
  def byBehavior(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior!="UNINSTALL")
      .map(log=>(log.behavior,1l))
      .keyBy(_._1)
      .timeWindow(Time.hours(1),Time.seconds(1))
      .process(new MarketingCountByBehavior)
      .print()

    env.execute("App Marketing by Behavior")
  }


}
