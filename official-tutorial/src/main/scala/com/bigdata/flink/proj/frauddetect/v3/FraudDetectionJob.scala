package com.bigdata.flink.proj.frauddetect.v3

import com.bigdata.flink.proj.frauddetect.common.AlertSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
 * 承接v2，小额体现后1分钟内发生大额体现，就发起报警，否则就删除之前状态。
 * 超时通过定时器控制。
 */
object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val name1 = "tx-source"
    val txDS = env.addSource(new TransactionSource).uid(name1).name(name1)

    val name2 = "fruad-detect"
    val detectedDS = txDS.keyBy(_.getAccountId).process(new FraudDetector).uid(name2).name(name2)

    val name3 = "alert-sink"
    detectedDS.addSink(new AlertSink).uid(name3).name(name3)

    env.execute("FraudDetectionJob")
  }

}
