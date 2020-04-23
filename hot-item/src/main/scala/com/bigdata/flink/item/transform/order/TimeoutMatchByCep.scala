package com.bigdata.flink.item.transform.order

import java.util
import com.bigdata.flink.item.bean.order.{OrderEvent, OrderResult}
import org.apache.flink.cep.{PatternFlatSelectFunction, PatternFlatTimeoutFunction}
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

class TimeoutMatchByCep extends PatternFlatSelectFunction[OrderEvent, OrderResult] with PatternFlatTimeoutFunction[OrderEvent, OrderResult] {
  override def flatSelect(map: util.Map[String, util.List[OrderEvent]], collector: Collector[OrderResult]): Unit = {
    val payOrder = map.getOrElse("follow",null)
    OrderResult(payOrder.iterator().next().orderId,"success")
  }

  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long, collector: Collector[OrderResult]): Unit = {
    val createdOrder = map.getOrElse("begin", null)
    OrderResult(createdOrder.iterator().next().orderId,"timeout")
  }

}
