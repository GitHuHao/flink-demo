package com.bigdata.flink.proj.frauddetect.v3

import com.bigdata.flink.proj.frauddetect.common.Alert
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Transaction

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.0
  val LARGE_AMOUNT: Double = 500.0
  val DURATION: Long = 60 * 1000L
}


class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {

  private var smallFlagState: ValueState[Boolean] = _
  private var timerState: ValueState[Long] = _


  override def open(parameters: Configuration): Unit = {
    smallFlagState = getRuntimeContext.getState(new ValueStateDescriptor("small-tx-happen", classOf[Boolean]))
    timerState = getRuntimeContext.getState(new ValueStateDescriptor("clear-timer", classOf[Long]))
  }

  override def processElement(i: Transaction, context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                              collector: Collector[Alert]): Unit = {
    val smallFlag = smallFlagState.value()
    if (smallFlag != null) {
      if (i.getAmount > FraudDetector.LARGE_AMOUNT) {
        collector.collect(Alert(i.getAccountId))
      }
      // 之前存在过小额交易，不管本次是否触发报警，都需要重新设置状态，删除定时器
      smallFlagState.clear()
      timerState.clear()
      context.timerService().deleteProcessingTimeTimer(timerState.value())
    }

    // 遇到小额交易，就注册定时器(时间上约束小额交易对欺诈检测有效期)，更新状态标记
    if (i.getAmount < FraudDetector.SMALL_AMOUNT) {
      val tmstp = context.timerService().currentProcessingTime() + FraudDetector.DURATION
      context.timerService().registerProcessingTimeTimer(tmstp)
      timerState.update(tmstp)
      smallFlagState.update(true)
    }
  }

  // 超时自动清理状态（之前的小额交易不再参与欺诈检查）
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                       out: Collector[Alert]): Unit = {
    smallFlagState.clear()
    timerState.clear()
  }

}
