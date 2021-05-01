package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.{OrderEvent, TxEvent}
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * 功能：订单和支付流式对账app
 * 难点：订单流和支付流到达先后顺序不一致，需要借助状态进行适配
 */
object OrderAndTxMatchApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val orderDS = env.readTextFile(CommonSuit.getFile("applog/OrderLog.csv"))
      .map{line =>
        val arr = line.split(",")
        OrderEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong)
      }

    val txDS = env.readTextFile(CommonSuit.getFile("applog/ReceiptLog.csv"))
      .map{line =>
        val arr = line.split(",")
        TxEvent(arr(0),arr(1),arr(2).toLong)
      }

    val connectDS = orderDS.connect(txDS)

    connectDS.keyBy("txId","txId")
      .process(new CoProcessFunction[OrderEvent,TxEvent,String] {
        private var orderMap:MapState[String,OrderEvent] = _
        private var txMap:MapState[String,TxEvent] = _

        override def open(parameters: Configuration): Unit = {
          val orderMapStateDesc = new MapStateDescriptor("orderMap",classOf[String],classOf[OrderEvent])
          orderMap = getRuntimeContext.getMapState(orderMapStateDesc)
          val txMapStateDesc = new MapStateDescriptor("txMap",classOf[String],classOf[TxEvent])
          txMap = getRuntimeContext.getMapState(txMapStateDesc)
        }

        override def processElement1(in1: OrderEvent, context: CoProcessFunction[OrderEvent, TxEvent, String]#Context,
                                     collector: Collector[String]): Unit = {
          if(txMap.contains(in1.txId)){
            val tx = txMap.get(in1.txId)
            collector.collect(s"${in1.orderId} ${tx.txId} 对账完毕")
            txMap.remove(in1.txId)
          }else{
            orderMap.put(in1.txId,in1)
          }
        }

        override def processElement2(in2: TxEvent, context: CoProcessFunction[OrderEvent, TxEvent, String]#Context,
                                     collector: Collector[String]): Unit = {
          if(orderMap.contains(in2.txId)){
            val order = orderMap.get(in2.txId)
            collector.collect(s"${order.orderId} ${in2.txId} 对账完毕")
            orderMap.remove(in2.txId)
          }else{
            txMap.put(in2.txId,in2)
          }
        }
    }).print()

    env.execute("OrderAndTxMatchApp")
  }

}
