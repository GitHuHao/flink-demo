package com.bigdata.flink.model

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class TimeWindow {

  /**
   * 滚动窗口：时间对其，窗口长度固定，无数据重叠
   * 在有元素输入情况下每5秒启动一次窗口长度为5秒的计算，无元素输入时，不计算
   * def timeWindow(size: Time): WindowedStream[T, K, TimeWindow]
   */
  @Test
  def tumblingWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0)
    keyedStream.timeWindow(Time.seconds(5))
      .reduce((t1,t2)=>(t1._1,t1._2+t2._2))
      .print()
    env.execute("tumblingWindow")
  }

  /**
   * 滑动窗口：时间对齐，窗口长度固定，有重复值
   * def timeWindow(size: Time, slide: Time): WindowedStream[T, K, TimeWindow]
   */
  @Test
  def slideWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0)
    keyedStream.timeWindow(Time.seconds(3),Time.seconds(2))
      .reduce((t1,t2)=>(t1._1,t1._2+t2._2))
      .print()
    env.execute("slideWindow")
  }

  // sessionWindowWithGap 需要引入 Watermark 机制

  /**
   * 基于滚动创建执行 fold 折叠操作
   * mapreduce by fold on trumbling window
   */
  @Test
  def wordcountByFold(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000, '\n', 3)
    val keyedStream = stream.map((_, 1)).keyBy(0)
    val streamWindow = keyedStream.timeWindow(Time.seconds(5))
    streamWindow.fold(("",0)){
      (acc,item) => (item._1,acc._2 + item._2)
    }.print()
    env.execute("fold on window")
  }

  /**
   * 滚动窗口上聚合求最大值
   */
  @Test
  def totalCountByFold(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.map(_.split("\\s+")).keyBy(0)
    val streamWindow = keyedStream.timeWindow(Time.seconds(5))
    streamWindow.max(1).map(arr => arr.mkString(",")).print()
    env.execute("max agg on tumnling window")
  }




}
