package com.bigdata.flink.proj.frauddetect.v1

import com.bigdata.flink.proj.frauddetect.common.AlertSink
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
 * 欺诈监控1：打印每一条交易id，侧重走通流程
 */
object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // WebUI 查看 flink-runtime-web_${scala.binary.version}
    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)

    val transactions = env.addSource(new TransactionSource)
      .name("transactions") // 给关键算子取名称

    val alerts = transactions.keyBy(_.getAccountId())
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts.addSink(new AlertSink())
      .name("send-alerts")

    env.execute("Fraud DetectionJob")
  }


}
