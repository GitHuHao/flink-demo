package com.bigdata.flink.item.bean.tx

case class OrderEvent(orderId:Long,eventType:String,txId:String,eventTime:Long)
