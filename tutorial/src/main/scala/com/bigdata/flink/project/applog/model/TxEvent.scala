package com.bigdata.flink.project.applog.model

case class TxEvent(var txId: String = null,
                   var payChannel: String = null,
                   var eventTime: Long = 0L)
