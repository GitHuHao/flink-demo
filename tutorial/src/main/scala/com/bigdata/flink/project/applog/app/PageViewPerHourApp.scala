package com.bigdata.flink.project.applog.app


import com.bigdata.flink.project.applog.model.UserBehavior
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.api.common.time.{Time => TtlTime}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date

/**
 * 统计小时级别页面访问PV
 * 注意这里要使用窗口思路，而不是wordcount思路计算
 * 2> 2017-11-26 09 新增访问数：41890 当日累计访问数: 41890
 * 2> 2017-11-26 10 新增访问数：48022 当日累计访问数: 48022
 * 2> 2017-11-26 11 新增访问数：47298 当日累计访问数: 47298
 * 2> 2017-11-26 12 新增访问数：44499 当日累计访问数: 44499
 * 2> 2017-11-26 13 新增访问数：48649 当日累计访问数: 48649
 * 2> 2017-11-26 14 新增访问数：50838 当日累计访问数: 50838
 * 2> 2017-11-26 15 新增访问数：52296 当日累计访问数: 52296
 * 2> 2017-11-26 16 新增访问数：52552 当日累计访问数: 52552
 * 2> 2017-11-26 17 新增访问数：48292 当日累计访问数: 48292
 * 2> 2017-11-26 18 新增访问数：13 当日累计访问数: 13
 */
object PageViewPerHourApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC) // 处理文件，适合用批模式
    val ds = env.readTextFile(CommonSuit.getFile("applog/UserBehavior.csv"))
    // 543462,1715,1464116,pv,1511658000
    ds.map{line=>
      val arr = line.split(",")
      UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong*1000L)
    }.filter(_.behavior.equals("pv"))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[UserBehavior] {
          override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = element.timestamp
        }
      )).keyBy(_.behavior)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .process(new ProcessWindowFunction[UserBehavior,String,String,TimeWindow] {

        private var userState:ValueState[Int] = _
        private var dateState:ValueState[String] = _
        private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        // TODO 设置状态过去时间
        override def open(parameters: Configuration): Unit = {
          val ttlConf = StateTtlConfig.newBuilder(TtlTime.days(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build()

          val userDesc = new ValueStateDescriptor("user",classOf[Int])
          userDesc.enableTimeToLive(ttlConf)

          val dateDesc = new ValueStateDescriptor("date",classOf[String])
          dateDesc.enableTimeToLive(ttlConf)

          userState = getRuntimeContext.getState(userDesc)
          dateState = getRuntimeContext.getState(dateDesc)
        }

        override def process(key: String, context: Context, elements: Iterable[UserBehavior], out: Collector[String]): Unit = {
          val currentDateTime = sdf.format(new Date(context.window.getStart))
          val lastDate = dateState.value()
          if(lastDate!=null){ // 切换日期时切换状态
            if(!lastDate.equals(currentDateTime.substring(0,10))){
              dateState.update(currentDateTime.substring(0,10))
              userState.clear()
            }
          }else{
            dateState.update(currentDateTime.substring(0,10))
          }
          val size1 = userState.value()
          var size2 = userState.value()
          elements.foreach(u => size2 += 1)
          out.collect(s"${currentDateTime.substring(0,13)} 新增访问数：${size2-size1} 当日累计访问数: ${size2}")
        }
      })
      .print()
    env.execute(getClass.getSimpleName)
  }

}
