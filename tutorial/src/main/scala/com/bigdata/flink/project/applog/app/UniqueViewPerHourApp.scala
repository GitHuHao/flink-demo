package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.UserBehavior
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.{Time => TtlTime}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date
import scala.collection.convert.ImplicitConversions.`iterator asScala`

/**
 * 统计小时级别页面访问uv
 */
object UniqueViewPerHourApp {

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

        private var userState:MapState[Long,String] = _
        private var dateState:ValueState[String] = _
        private val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

        //
        /**
         * TODO 状态清除3种方式
         * 1.设置 ttl，从创建或更新开始倒计时（缺点：设置的24h，是真实24h，而不是基于事件时间推算，批处理一段时间内数据，造成存储开销巨大）
         *  override def open(parameters: Configuration): Unit = {
         *      val ttlConf = StateTtlConfig.newBuilder(TtlTime.days(1))
         *            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build()
         *
         *      val userDesc = new MapStateDescriptor("user-id",classOf[Long],classOf[String])
         *      userDesc.enableTimeToLive(ttlConf)
         *      ...
         *  }
         *
         * 2.基于事件时间，设置定时任务，在定时任务中清理(优点：批处理连续一段时间数据，可以让状态快速失效)
         *  override def processElement(i: AdsClickLog, context: KeyedProcessFunction[(Long, Long), AdsClickLog, String]#Context,
                                    collector: Collector[String]): Unit = {
         *      val now = context.timestamp()
                val zone = ZoneOffset.ofHours(8)
                val today = LocalDateTime.ofEpochSecond(now / 1000, 0, zone).toLocalDate
                val tomorrow = LocalDateTime.of(today.plusDays(1),LocalTime.of(0,0,0)).toEpochSecond(zone)
                context.timerService().registerEventTimeTimer(tomorrow)
                .....
             }

            override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdsClickLog, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
                warnState.clear()
                clickState.clear()
            }
         *
         * 3.processElement中处理之前，先比较时间是否触发清除逻辑，然后执行清除，在执行处理逻辑 （可行，但状态生命周期维护与数据处理逻辑耦合）
         * override def processElement(i: AdsClickLog, context: KeyedProcessFunction[(Long, Long), AdsClickLog, String]#Context,
                                    collector: Collector[String]): Unit = {
         *
         * @param parameters
         */
        override def open(parameters: Configuration): Unit = {
          val ttlConf = StateTtlConfig.newBuilder(TtlTime.days(1))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build()

          val userDesc = new MapStateDescriptor("user-id",classOf[Long],classOf[String])
          userDesc.enableTimeToLive(ttlConf)

          val dateDesc = new ValueStateDescriptor[String]("date",classOf[String])
          dateDesc.enableTimeToLive(ttlConf)

          userState = getRuntimeContext.getMapState(userDesc)
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
          val size1 = userState.keys().iterator().size
          elements.foreach(u => userState.put(u.userId,null))
          val size2 = userState.keys().iterator().size
          out.collect(s"${currentDateTime.substring(0,13)} 新增独立访客：${size2-size1} 当日累计独立访客: ${size2}")
        }
      })
      .print()
    env.execute(getClass.getSimpleName)
  }

}
