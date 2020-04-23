package com.bigdata.flink.item.app

import com.bigdata.flink.item.bean.behavior.UserBehavior
import com.bigdata.flink.item.transform.uv.{UvCountByWindow, UvCountWithBloomByElement, UvCountWithBloomByWindow, UvTriggerByElement, UvTriggerByWindow}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class UvAnalysis {

  @Test
  def hourlyUv1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getResource("/input/UserBehavior.csv").getPath
    env.readTextFile(path)
      .map{line=>
        val strings = line.split(",")
        UserBehavior(strings(0).toLong,strings(1).toLong,strings(2).toInt,
          strings(3),strings(4).toLong*1000)
      }.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior=="pv")
      .timeWindowAll(Time.seconds(60*60))
      .apply(new UvCountByWindow)
      .print()

    env.execute("User View Job")
  }

  @Test
  def hourlyUvByElement(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getResource("/input/UserBehavior.csv").getPath
    env.readTextFile(path)
      .map{line=>
        val strings = line.split(",")
        UserBehavior(strings(0).toLong,strings(1).toLong,strings(2).toInt,
          strings(3),strings(4).toLong*1000)
      }.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior=="pv")
      .keyBy(_.userId)
      .timeWindow(Time.seconds(60*60))
      .trigger(new UvTriggerByElement())
      .process(new UvCountWithBloomByElement())
      .print()

    env.execute("User View Job")
  }
}
