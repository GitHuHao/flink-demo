package com.bigdata.flink.item.transform.market

import java.util.UUID
import java.util.concurrent.TimeUnit

import com.bigdata.flink.item.bean.market.MarketingUserBehavior
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class SimulatedEventSource extends RichParallelSourceFunction[MarketingUserBehavior]{

  var running = true

  var channels = Seq("AppStore", "XiaomiStore", "HuaweiStore", "weibo", "wechat", "tieba")
  val behaviors = Seq("BROWSE", "CLICK", "PURCHASE", "UNINSTALL")

  val rand = new Random

  override def run(sourceContext: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    val maxElements = Long.MaxValue
    var count = 0l
    while(running && count < maxElements){
      val userId = UUID.randomUUID().toString
      val behavior = behaviors(rand.nextInt(behaviors.size))
      val channel = channels(rand.nextInt(channels.size))
      val tmstp = System.currentTimeMillis()
      sourceContext.collectWithTimestamp(MarketingUserBehavior(userId,behavior,channel,tmstp),tmstp)
      count += 1
      TimeUnit.MILLISECONDS.sleep(5l)
    }
  }

  override def cancel(): Unit = false

}
