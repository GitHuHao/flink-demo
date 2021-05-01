package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.LoginEvent
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import java.time.Duration
import java.util

object LoginFailAlertByCEPApp {

  /**
   * 15秒内连续两次登录失败，失败为恶意登录
   * 8> {first=[LoginEvent(1035,83.149.9.216,fail,1558430842000), LoginEvent(1035,83.149.9.216,fail,1558430843000)]}
   * 8> {first=[LoginEvent(1035,83.149.9.216,fail,1558430843000), LoginEvent(1035,83.149.24.26,fail,1558430844000)]}
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyedDS = env.readTextFile(CommonSuit.getFile("applog/LoginLog.csv"))
      .map{line =>
        val arr = line.split(",")
        LoginEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong*1000L)
      }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner(
      new SerializableTimestampAssigner[LoginEvent] {
        override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.eventTime
      }
    )).keyBy(_.userId)

    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[LoginEvent](){
        override def filter(t: LoginEvent): Boolean = t.eventType.equals("fail")
      }).times(2)
      .consecutive()
      .within(Time.seconds(15))

    val filterDS = CEP.pattern(keyedDS, pattern)
      .select(new PatternSelectFunction[LoginEvent,String](){
        override def select(map: util.Map[String, util.List[LoginEvent]]): String = map.toString
      })

    filterDS.print()

    env.execute(getClass.getSimpleName)
  }

}
