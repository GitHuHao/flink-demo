package com.bigdata.flink.item.transform.login

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import scala.collection.JavaConversions._
import com.bigdata.flink.item.bean.login.{LoginEvent, Warning}
import org.apache.flink.cep.PatternSelectFunction

class FailMatchByCep extends PatternSelectFunction[LoginEvent,Warning]{
  private lazy val sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")

  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    val first = map.getOrElse("begin", null).iterator.next()
    val second = map.getOrElse("next", null).iterator.next()
    val startTime = sdf.format(new Date(first.eventTime))
    val endTime = sdf.format(new Date(second.eventTime))
    val msg = s"login fail 2 times within 2 seconds"
    Warning(second.userId,startTime,endTime,msg)
  }
}
