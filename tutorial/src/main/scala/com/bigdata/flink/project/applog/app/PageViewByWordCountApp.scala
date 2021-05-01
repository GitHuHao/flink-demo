package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.UserBehavior
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala._

// wordcount思路实现pv统计
object PageViewByWordCountApp {

  // (pv,434349)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC) // 处理文件，适合用批模式
//    val ds = env.readTextFile(getClass.getClassLoader.getResource("applog/UserBehavior.csv").toString)
    val ds = env.readTextFile(CommonSuit.getFile("applog/UserBehavior.csv"))
    // 543462,1715,1464116,pv,1511658000
    ds.map{line=>
      val arr = line.split(",")
      UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
    }.filter(_.behavior.equals("pv"))
      .map(_ => ("pv",1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("PageViewApp")
  }

}
