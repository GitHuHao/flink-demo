package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.source.AppMarketingDataSource
import org.apache.flink.streaming.api.scala._

/**
 * 渠道行为统计
 */
object BebaviorSummaryByChannelApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new AppMarketingDataSource())
      .map(log => (s"${log.channel}:${log.behavior}",1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("BebaviorSummaryByChannelApp")
  }

}
