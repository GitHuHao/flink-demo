package com.bigdata.flink.project.applog.model

case class ApacheLog(var ip:String = null,
                var eventTime:Long = 0L,
                var method:String= null,
                var url:String= null
               )

