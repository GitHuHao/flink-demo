package com.bigdata.flink.project.applog.source

import com.bigdata.flink.project.applog.model.MarketingUserBehavior
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

import java.util.concurrent.TimeUnit
import scala.util.Random

class AppMarketingDataSource extends RichSourceFunction[MarketingUserBehavior] {
  private var running: Boolean = false
  private val random = new Random()

  override def open(parameters: Configuration): Unit = {
    running = true
  }

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val channels = Seq("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo")
    val behaviors = Seq("download", "install", "update", "uninstall")
    while (running) {
      val mub = MarketingUserBehavior(
        random.nextInt(1000000),
        behaviors(random.nextInt(behaviors.size)),
        channels(random.nextInt(channels.size)),
        System.currentTimeMillis()
      )
      sourceContext.collect(mub)
      TimeUnit.SECONDS.sleep(2)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
