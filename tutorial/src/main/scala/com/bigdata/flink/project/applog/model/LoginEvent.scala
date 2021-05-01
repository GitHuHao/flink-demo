package com.bigdata.flink.project.applog.model

case class LoginEvent(var userId: Long = 0L,
                      var ip: String = null,
                      var eventType: String = null,
                      var eventTime: Long = 0L)
