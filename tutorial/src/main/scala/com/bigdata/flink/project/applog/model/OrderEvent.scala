package com.bigdata.flink.project.applog.model

case class OrderEvent(var orderId: Long = 0L,
                      var eventType: String = null,
                      var txId: String = null,
                      var eventTime: Long = 0L)
