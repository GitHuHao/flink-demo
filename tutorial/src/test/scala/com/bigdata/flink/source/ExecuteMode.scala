package com.bigdata.flink.source

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._
import org.junit.Test

/**
 * 数据处理模式，Stream、Batch、Automic
 * Streaming 默认处理方式，既可以处理批又可以处理流，且处理批时处理一条输出一条
 * Batch 只能处理批，比Stream批处理效率高，一次性处理完再输出
 * Automic 自动决定按Stream还是Batch处理
 * 注: 不推荐在代码中写死处理模式，推荐提交任务时通过-Dexecution.runtime-mode指定处理模式
 * bin/flink run -Dexecution.runtime-mode=BATCH -j xx.jar -c MAIN_CLASS
 */
class ExecuteMode {

  /**
   * 默认为Stream模型
   * 7> (apple,1)
   * 3> (hello,1)
   * 2> (java,1)
   * 2> (java,2)
   * 3> (hello,2)
   * 3> (python,1)
   */
  @Test
  def streamMode(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.readTextFile(CommonSuit.getFile("wc/1.txt"))
    ds.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1).print()
    env.execute("streamMode")
  }

  /**
   * 设置批处理
   * 2> (java,2)
   * 3> (hello,2)
   */
  @Test
  def batchMode(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val ds = env.readTextFile(CommonSuit.getFile("wc/1.txt"))
    ds.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1).print()
    env.execute("batchMode")
  }

  /**
   * AUTOMATIC 按批处理
   * 3> (hello,2)
   * 2> (java,2)
   */
  @Test
  def automic1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    val ds = env.readTextFile(CommonSuit.getFile("wc/1.txt"))
    ds.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1).print()
    env.execute("automic")
  }

  /**
   * AUTOMATIC 按流处理
   * 2> (ho,1)
   * 3> (hello,1)
   * 3> (hello,2)
   * 2> (ho,2)
   * 3> (hello,3)
   */
  @Test
  def automic2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC)
    val ds = env.socketTextStream("localhost",9000)
    ds.flatMap(_.split("\\s+")).map((_,1)).keyBy(0).sum(1).print()
    env.execute("automic2")
  }

}
