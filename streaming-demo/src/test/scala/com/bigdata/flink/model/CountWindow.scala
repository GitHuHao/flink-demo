package com.bigdata.flink.model

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.streaming.api.scala._
import org.junit.Test

class CountWindow {

  val localFile = CommonSuit.getFile("1.txt")
  val s2File = "hdfs://hadoop01:9000/apps/mr/wc/in.1.txt"

  /**
   * 每接收两个元素，就执行 reduce 操作,即便两个元素不同
   * def countWindowAll(size: Long): AllWindowedStream[T, GlobalWindow]
   *
   * 1.先启动发送端 nc -l 90000
   * 2.然后启动app
   * 3.app 停止，发送端自动退出
   */
  @Test
  def countWindowAll1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\s+")).map((_,1)).keyBy(0)
    keyedStream.countWindowAll(2).reduce((t1,t2)=> (s"${t1._1} ${t2._1}",t1._2+t2._2)).print()
    env.execute("countWindowAll1")
  }

  /**
   *  每接收 slide 个元素，启动一次长度为size的窗口计算
   *  def countWindowAll(size: Long, slide: Long): AllWindowedStream[T, GlobalWindow]
   *
   */
  @Test
  def countWindowAll2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0)
    keyedStream.countWindowAll(5, 2).reduce((t1,t2)=> (s"${t1._1} ${t2._1}",t1._2+t2._2)).print()
    env.execute("countWindowAll2")
  }

  /**
   * countWindow 对keyedStream 当某key元素个数达到2个,就对此元素执行reduce操作
   */
  @Test
  def countWindow1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyedStream = env.socketTextStream("localhost", 9000).flatMap(_.split("\\s+")).map((_,1)).keyBy(0)
    keyedStream.countWindow(2).reduce((t1,t2)=>(s"${t1._1} ${t2._1}",t1._2+t2._2)).print()
    env.execute("countWindow1")
  }

  /**
   * def countWindow(size: Long, slide: Long): WindowedStream[T, K, GlobalWindow]
   * 每接收 slide个数相同元素，开始启动一次长度为 size 的窗口计算
   */
  @Test
  def countWindow2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", 9000)
    val keyedStream = stream.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0)
    keyedStream.countWindow(3,2).reduce((t1,t2) => (t1._1,t1._2+t2._2)).print()
    env.execute("countWindow2")
  }

}
