package com.bigdata.flink.project.applog.model

case class AdClickCount (var province: String = null,
                          var adId: Long = 0L,
                         var count: Long = 0L,
                         var windowEnd: Long = 0L)