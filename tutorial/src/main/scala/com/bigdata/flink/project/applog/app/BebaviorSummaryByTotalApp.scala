package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.source.AppMarketingDataSource
import org.apache.flink.streaming.api.scala._

/**
 * 不分渠道统计行为操作次数
 */
object BebaviorSummaryByTotalApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new AppMarketingDataSource)
      .map(log => (log.behavior,1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("BebaviorSummaryByTotalApp")
  }

}
