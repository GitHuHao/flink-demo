package com.bigdata.flink.project.applog.model

case class MarketingUserBehavior(var userId: Long = 0L,
                                 var behavior: String = null,
                                 var channel: String = null,
                                 var timestamp: Long = 0L
                                )
