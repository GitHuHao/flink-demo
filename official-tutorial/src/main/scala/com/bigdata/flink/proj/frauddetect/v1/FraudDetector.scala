package com.bigdata.flink.proj.frauddetect.v1

import com.bigdata.flink.proj.frauddetect.common.Alert
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Transaction

class FraudDetector extends KeyedProcessFunction[Long,Transaction,Alert]{
  override def processElement(i: Transaction, context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                              collector: Collector[Alert]): Unit = {
    collector.collect(Alert(i.getAccountId))
  }
}
