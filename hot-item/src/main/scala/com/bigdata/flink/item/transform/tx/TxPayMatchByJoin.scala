package com.bigdata.flink.item.transform.tx

import com.bigdata.flink.item.bean.tx.{OrderEvent, ReceiptEvent}
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector

class TxPayMatchByJoin extends ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  override def processElement(order: OrderEvent, receipt: ReceiptEvent, context: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    collector.collect(order,receipt)
  }
}
