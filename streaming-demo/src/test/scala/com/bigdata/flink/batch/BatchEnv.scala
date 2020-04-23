package com.bigdata.flink.batch

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.scala._
import org.junit.Test

class BatchEnv {

  val localFile:String = CommonSuit.getFile("1.txt")
  val s3File:String = "hdfs://hadoop01:9000/apps/mr/wc/in/1.txt"

  /**
   * ExecutionEnvironment 创建Batch 环境
   * 无需使用 env.execute() 启动
   */
  @Test
  def wordCount(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile(localFile)
    stream.flatMap(_.split("\\s+")).map((_,1)).groupBy(0).sum(1).print()
  }

}
