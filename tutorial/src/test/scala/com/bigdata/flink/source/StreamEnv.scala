package com.bigdata.flink.source

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class StreamEnv {

  val localFile:String = CommonSuit.getFile("1.txt")
  val s3File:String = "hdfs://hadoop01:9000/apps/mr/wc/in/1.txt"

  /**
   * StreamExecutionEnvironment 创建流式环境
   * 不配置窗口情况默认每次对当前输入元素的全量进行wordcount
   */
  @Test
  def wordcountWithoutWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost",9000,'\n',3)
    stream.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1).print()
    env.execute("wordCount")
  }

  /**
   * 设置时间滚动窗口，按窗口长度统计
   * keyedStream.timeWindow(size)
   */
  @Test
  def wordcountWithTumblingWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost",9000,'\n',3)
    val keyedStream = stream.flatMap(_.split("\\W+")).map((_,1)).keyBy(0)
    val tumblingWindowStream = keyedStream.timeWindow(Time.seconds(5))
    tumblingWindowStream.sum(1).print()
    env.execute("wordCount")
  }

  /**
   * 设置时间滑动窗口，每滑动2s，统计最近4s长度
   * keyedStream.timeWindow(size,slide)
   */
  @Test
  def wordcountWithSlidingWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\W+")).map((_, 1)).keyBy(0)
    val sidingWindowStream = keyedStream.timeWindow(Time.seconds(4), Time.seconds(2))
    sidingWindowStream.sum(1).print()
    env.execute("wordCountWithSlidingWindow")
  }

  /**
   * 设置 所有元素计数滚动窗口，每收集4个元素，进行一次统计
   */
  @Test
  def wordcountWithCountWindowAll(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\W+")).filter(_.nonEmpty).map((_, 1)).keyBy(0)
    val countWindowStream = keyedStream.countWindowAll(4)
    countWindowStream.sum(1).print()
    env.execute("wordcountWithCountWindow")
  }



}
