package com.bigdata.flink.source

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.streaming.api.scala._
import org.junit.Test

class WordCount {

  @Test
  def wordCount(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.readTextFile(CommonSuit.getFile("wc/1.txt"))
    val wc = ds.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1)
    wc.print()
    env.execute("WordCount App")
  }

  @Test
  def streamWordCount(): Unit ={
    // 默认会进行累加
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.socketTextStream("localhost", 9001,maxRetry = 3)
    val wc = ds.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1)
    wc.print()
    env.execute("Stream WordCount App")
  }

}
