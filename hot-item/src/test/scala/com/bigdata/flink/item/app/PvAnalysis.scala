package com.bigdata.flink.item.app

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.behavior.UserBehavior
import com.bigdata.flink.item.transform.pv.PvCountByWindow
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class PvAnalysis {

  @Test
  def hourPv1(): Unit = {
    val path = getClass.getResource("/input/UserBehavior.csv").getPath
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val sdf = new SimpleDateFormat("yyyy年MM月dd日 HH时")

    val stream = env.readTextFile(path).map{line =>
      val strings = line.split(",")
      UserBehavior(strings(0).toLong,strings(1).toLong,strings(2).toInt,
        strings(3),strings(4).toLong*1000)
    }.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior=="pv")
      .map(behavior=>(sdf.format(new Date(behavior.timestamp)),1))
      .keyBy(_._1)
      .timeWindow(Time.seconds(60*60))
      .sum(1)

    stream.print()

    env.execute("Page View Job")
  }

  @Test
  def hourPv2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getResource("/input/UserBehavior.csv").getPath
    env.readTextFile(path).map{line =>
      val strings = line.split(",")
      UserBehavior(strings(0).toLong,strings(1).toLong,strings(2).toInt,strings(3),strings(4).toLong*1000)
    }.assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior=="pv")
      .timeWindowAll(Time.seconds(60*60))
      .apply(new PvCountByWindow) // apply = sum + window
      .print()
    env.execute("Page View Job")
  }


}
