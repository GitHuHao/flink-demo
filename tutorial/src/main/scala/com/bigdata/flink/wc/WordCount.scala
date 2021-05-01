package com.bigdata.flink.wc

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * 批处理
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val ds = env.readTextFile(CommonSuit.getFile("wc/1.txt"))
    val wc = ds.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).sum(1)
    wc.print()
    env.execute("WordCount Job")
  }

}
