package com.bigdata.flink.project.applog.model

case class UserBehavior(
                         var userId: Long = 0L,
                         var itemId: Long = 0L,
                         var categoryId: Integer = null,
                         var behavior: String = null,
                         var timestamp: Long = 0L
                       )
