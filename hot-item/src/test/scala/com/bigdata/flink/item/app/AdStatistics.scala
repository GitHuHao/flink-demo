package com.bigdata.flink.item.app

import com.bigdata.flink.item.bean.ad.AdClickLog
import com.bigdata.flink.item.bean.black.BlackListWarning
import com.bigdata.flink.item.transform.ad.{CountAgg, CountResult}
import com.bigdata.flink.item.transform.black.FilterBlackListUser
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class AdStatistics {

  @Test
  def byGeo(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getResource("/input/AdClickLog.csv").getPath

    env.readTextFile(path).map{line =>
      val strings = line.split(",")
      AdClickLog(strings(0).toLong,strings(1).toLong,strings(2),strings(3),strings(4).toLong*1000)
    }.assignAscendingTimestamps(_.timestamp)
      .keyBy(_.province)
      .timeWindow(Time.minutes(60),Time.seconds(5))
      .aggregate(new CountAgg,new CountResult)
      .print()

    env.execute("Ad Statistics by Geo")
  }

  @Test
  def byGeoWithBlackFilter(): Unit ={
    val blackListOutputTag = new OutputTag[BlackListWarning]("blacklist")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val path = getClass.getResource("/input/AdClickLog.csv").getPath
    val markedStream = env.readTextFile(path).map{line =>
      val strings = line.split(",")
      AdClickLog(strings(0).toLong,strings(1).toLong,strings(2),strings(3),strings(4).toLong*1000)
    }.assignAscendingTimestamps(_.timestamp)

    val filterBlackStream = markedStream.keyBy(log=>(log.userId,log.adId))
      .process(new FilterBlackListUser(100,blackListOutputTag))

    val adCountStream = filterBlackStream.keyBy(_.province)
      .timeWindow(Time.minutes(60),Time.seconds(5))
      .aggregate(new CountAgg,new CountResult)

    adCountStream.print()

    filterBlackStream.getSideOutput(blackListOutputTag).print("black list")

    env.execute("Ad Statistics by Geo With Black Filter")
  }



}
