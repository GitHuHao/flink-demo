package com.bigdata.flink.project.applog.model

case class PageCount(var url: String = null,
                     var count: Long = 0L,
                     var windowEnd: Long = 0L)