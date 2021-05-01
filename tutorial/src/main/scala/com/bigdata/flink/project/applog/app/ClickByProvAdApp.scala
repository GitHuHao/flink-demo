package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.AdsClickLog
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

/**
 * 省份、广告维度统计点击量
 */
object ClickByProvAdApp {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = ExecutionEnvironment.getExecutionEnvironment
//    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val ds = env.readTextFile(CommonSuit.getFile("applog/AdClickLog.csv"))
    val logDS = ds.map{line =>
      val arr = line.split(",")
      AdsClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
    }

    val xx = logDS.filter(l => l.adId==36156)

    xx.map(log => (log.province,1))
      .groupBy(0)
      .sum(1)
      .print()

//    env.execute("ClickByProvAdApp")
  }

  /**
   * 来一条算一条，数据不会丢
   */
  def test1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.STREAMING)
    val ds = env.readTextFile(CommonSuit.getFile("applog/AdClickLog.csv"))
    val logDS = ds.map{line =>
      val arr = line.split(",")
      AdsClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
    }

    val xx = logDS.filter(l => l.adId==36156)

    xx.map(log => (log.province,1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("ClickByProvAdApp")
  }

  /**
   * 只有1条记录的会丢失
   */
  def test2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.BATCH)
    val ds = env.readTextFile(CommonSuit.getFile("applog/AdClickLog.csv"))
    val logDS = ds.map{line =>
      val arr = line.split(",")
      AdsClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
    }

    val xx = logDS.filter(l => l.adId==36156)

    xx.map(log => (log.province,1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute("ClickByProvAdApp")
  }

  /**
   * 批处理api 数据不会丢
   */
  def test3(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.readTextFile(CommonSuit.getFile("applog/AdClickLog.csv"))
    val logDS = ds.map{line =>
      val arr = line.split(",")
      AdsClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong)
    }

    val xx = logDS.filter(l => l.adId==36156)

    xx.map(log => (log.province,1))
      .groupBy(0)
      .sum(1)
      .print()
  }

}
