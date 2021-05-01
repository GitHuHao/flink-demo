package com.bigdata.flink.project.applog.model

case class AdsClickLog(var userId: Long = 0L,
                       var adId: Long = 0L,
                       var province: String = null,
                       var city: String = null,
                       var timestamp: Long = 0L)
