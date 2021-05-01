package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.{AdsClickLog}
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import java.time.{Duration, LocalDateTime, LocalTime, ZoneOffset}

/**
 * 每日广告点击超过100次定义为黑名单用户，之后点击不计入统计
 */
object AdClickBlackListApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[AdsClickLog] {
          override def extractTimestamp(element: AdsClickLog, recordTimestamp: Long): Long = element.timestamp
        })

    val blackListTag = new OutputTag[String]("blacklist")
    val mainDS = env.readTextFile(CommonSuit.getFile("applog/AdClickLog.csv"))
      .map{line =>
        val arr = line.split(",")
        AdsClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong*1000L)
      }.assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(i => (i.userId,i.adId))
      .process(new KeyedProcessFunction[(Long,Long),AdsClickLog,String] {

        private var warnState:ValueState[Boolean] = _
        private var clickState:ValueState[Int] = _
        private val maxClick:Int = 2

        override def open(parameters: Configuration): Unit = {
          warnState = getRuntimeContext.getState(new ValueStateDescriptor("warn",classOf[Boolean]))
          clickState = getRuntimeContext.getState(new ValueStateDescriptor("click",classOf[Int],0))
        }

        override def processElement(i: AdsClickLog, context: KeyedProcessFunction[(Long, Long), AdsClickLog, String]#Context,
                                    collector: Collector[String]): Unit = {
          if(clickState.value() == null){
            // 基于事件时间设置定时器，零点清除头一天状态
            val now = context.timestamp()
            val zone = ZoneOffset.ofHours(8)
            val today = LocalDateTime.ofEpochSecond(now / 1000, 0, zone).toLocalDate
            val tomorrow = LocalDateTime.of(today.plusDays(1),LocalTime.of(0,0,0)).toEpochSecond(zone)
            context.timerService().registerEventTimeTimer(tomorrow)
            clickState.update(1)
            collector.collect(s"${i.userId} click ad ${i.adId} ${clickState.value()} times")
          }else if(clickState.value() < maxClick){
            clickState.update(clickState.value() + 1)
            collector.collect(s"${i.userId} click ad ${i.adId} ${clickState.value()} times")
          }else{
            if(!warnState.value()){
              context.output(blackListTag,s"${i.userId} click ad ${i.adId} over ${maxClick} times")
              warnState.update(true)
            }
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdsClickLog, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
          warnState.clear()
          clickState.clear()
        }
      })

//    mainDS.print("ok")
    mainDS.getSideOutput(blackListTag).print("black")

    env.execute(getClass.getSimpleName)
  }

}
