package com.bigdata.flink.item.transform.tx

import com.bigdata.flink.item.bean.tx.{OrderEvent, ReceiptEvent}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

class TxMatchDetection(unmatchedPays:OutputTag[OrderEvent],unmatchedReceipts:OutputTag[ReceiptEvent]) extends CoProcessFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)]{
  private lazy val payState:ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("pay-state",classOf[OrderEvent]))
  private lazy val receiptState:ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("receipt-state",classOf[ReceiptEvent]))

  override def processElement1(pay: OrderEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val receipt = receiptState.value()
    if(receipt!=null){
      receiptState.clear()
      collector.collect((pay,receipt))
    }else{
      payState.update(pay)
      context.timerService().registerEventTimeTimer(pay.eventTime)
    }
  }

  override def processElement2(receipt: ReceiptEvent, context: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, collector: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay = payState.value()
    if(pay!=null){
      payState.clear()
      collector.collect((pay,receipt))
    }else{
      receiptState.update(receipt)
      context.timerService().registerEventTimeTimer(receipt.eventTime)
    }
  }

  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    val pay = payState.value()
    val receipt = receiptState.value()
    if(pay!=null) {
      ctx.output(unmatchedPays, pay)
    }
    if(receipt!=null){
      ctx.output(unmatchedReceipts,receipt)
    }
    payState.clear()
    receiptState.clear()
  }

}
