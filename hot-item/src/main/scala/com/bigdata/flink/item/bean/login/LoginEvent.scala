package com.bigdata.flink.item.bean.login

case class LoginEvent(userId:Long,ip:String,eventType:String,eventTime:Long)
