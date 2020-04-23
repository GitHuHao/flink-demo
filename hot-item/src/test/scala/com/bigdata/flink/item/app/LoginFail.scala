package com.bigdata.flink.item.app

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.login.{LoginEvent, Warning}
import com.bigdata.flink.item.bean.url.ApacheLogEvent
import com.bigdata.flink.item.transform.login.{FailMatchByCep, FailMatchByCompare, FailMatchByTimer}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class LoginFail {

  @Test
  def monitorByTimer(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getResource("/input/LoginLog.csv").getPath
    env.readTextFile(path)
      .map{line=>
        val strings = line.split(",")
        LoginEvent(strings(0).toLong,strings(1),strings(2),strings(3).toLong*1000)
      }.assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
          override def extractTimestamp(event: LoginEvent): Long = event.eventTime
        }
      ).keyBy(_.userId)
      .process(new FailMatchByTimer(2*1000))
      .print()

    env.execute("Login Fail Monitor")
  }

  @Test
  def monitorByCompare(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getResource("/input/LoginLog.csv").getPath
    env.readTextFile(path)
      .map{line=>
        val strings = line.split(",")
        LoginEvent(strings(0).toLong,strings(1),strings(2),strings(3).toLong*1000)
      }.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(event: LoginEvent): Long = event.eventTime
      }
    ).keyBy(_.userId)
      .process(new FailMatchByCompare)
      .print()

    env.execute("Login Fail Monitor")
  }

  @Test
  def loginFailWithCep(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val path = getClass.getResource("/input/LoginLog.csv").getPath
    val loginEventStream = env.readTextFile(path)
      .map{line =>
        val strings = line.split(",")
        LoginEvent(strings(0).toLong,strings(1),strings(2),strings(3).toLong*1000)
      }.assignTimestampsAndWatermarks(
        new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.milliseconds(3000)) {
        override def extractTimestamp(event: LoginEvent): Long = event.eventTime
      })

    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType=="fail")
      .next("next") // 严格紧邻匹配
      .where(_.eventType=="fail")
      .within(Time.seconds(2))

    val patternStream = CEP.pattern(loginEventStream.keyBy(_.userId),loginFailPattern)

    val loginFailStream = patternStream.select(new FailMatchByCep())

    loginFailStream.print()

    env.execute("Login Fail With Cep")
  }






}
