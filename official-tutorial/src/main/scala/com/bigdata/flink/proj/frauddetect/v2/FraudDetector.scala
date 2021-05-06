package com.bigdata.flink.proj.frauddetect.v2

import com.bigdata.flink.proj.frauddetect.common.Alert
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Transaction

/**
 * 伴生类对象
 * 对象记录常量信息
 * 类开发功能信息
 */
object FraudDetector{
  val SMALL_AMOUNT:Double=1.0
  val LARGE_AMOUNT:Double=500.0
}

class FraudDetector extends KeyedProcessFunction[Long,Transaction,Alert]{

  // 键控状态
  private var smallTxHappenedState:ValueState[Boolean] = _

  override def open(parameters: Configuration): Unit = {
    // unset (null)、true、false
    smallTxHappenedState = getRuntimeContext.getState(new ValueStateDescriptor("small-tx-happened",classOf[Boolean]))
    // open 中不能执行state的update clear操作
    // ValueStateDescriptor 管理状态的元数据信息
  }

  override def processElement(i: Transaction, context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                              collector: Collector[Alert]): Unit = {
    val smallTxHappened = smallTxHappenedState.value()
    if(smallTxHappened!=null){ // 只可能是 unset（null） 或 true，false情况不用
      if(i.getAmount() > FraudDetector.LARGE_AMOUNT){
        collector.collect(Alert(i.getAccountId))
      }
      smallTxHappenedState.clear()
    }
    if(i.getAmount < FraudDetector.SMALL_AMOUNT){
      smallTxHappenedState.update(true)
    }
  }
}
