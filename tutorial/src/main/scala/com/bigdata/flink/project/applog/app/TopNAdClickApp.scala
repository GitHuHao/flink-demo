package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.{AdClickCount, AdsClickLog}
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.time.Duration
import scala.collection.convert.ImplicitConversions.`iterator asScala`

/**
 * 滑动窗口每隔10秒统计最近1小时，地区广告点击数统计
 *
 *
 */
object TopNAdClickApp {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[AdsClickLog] {
          override def extractTimestamp(element: AdsClickLog, recordTimestamp: Long): Long = element.timestamp
        }
    )
    val lateOutput = new OutputTag[AdsClickLog]("late-AdsClickLog")
    val mainDS = env.readTextFile(CommonSuit.getFile("applog/AdClickLog.csv"))
      .map{line =>
        val arr = line.split(",")
        AdsClickLog(arr(0).toLong,arr(1).toLong,arr(2),arr(3),arr(4).toLong*1000L)
      }.assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(i => (i.province,i.adId))
      .window(TumblingEventTimeWindows.of(Time.hours(1),Time.seconds(10)))
      .allowedLateness(Time.seconds(10)) // 窗口触发计算后，水印增加10秒后再关闭
      .sideOutputLateData(lateOutput) // 迟到数据测输出流输出
      .aggregate( // provience,adId 维度 点击数累计
        new AggregateFunction[AdsClickLog,Long,Long] {
          override def createAccumulator(): Long = 0L

          override def add(value: AdsClickLog, accumulator: Long): Long = accumulator + 1L

          override def getResult(accumulator: Long): Long = accumulator

          override def merge(a: Long, b: Long): Long = a + b
        },
        new ProcessWindowFunction[Long,AdClickCount,(String,Long),TimeWindow] { // 统计好的结果结合窗口信息，封装AdClickCount对象
          override def process(key: (String, Long), context: Context, elements: Iterable[Long],
                               out: Collector[AdClickCount]): Unit = {
            out.collect(AdClickCount(key._1,key._2,elements.head,context.window.getEnd))
          }
        }
      ).keyBy(_.windowEnd) // 收集同一个窗口统计结果
      .process(new KeyedProcessFunction[Long,AdClickCount,String] { // 取窗口中的topN

        private var timerState:ValueState[Long] = _
        private var topNState:ListState[AdClickCount] = _

        override def open(parameters: Configuration): Unit = {
          val timerDesc = new ValueStateDescriptor("timer",classOf[Long])
          val topNDesc = new ListStateDescriptor("topN-adcount",classOf[AdClickCount])
          timerState = getRuntimeContext.getState(timerDesc)
          topNState = getRuntimeContext.getListState(topNDesc)
        }

        override def processElement(i: AdClickCount, context: KeyedProcessFunction[Long, AdClickCount, String]#Context,
                                    collector: Collector[String]): Unit = {
          if(timerState.value()==null){ //分组首个元素进入时设置定时器，延迟10秒，再统计topN
            context.timerService().registerEventTimeTimer(i.windowEnd+10L)
            timerState.update(i.windowEnd)
          }
          topNState.add(i) // 收集key相同统计结果
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, AdClickCount, String]#OnTimerContext,
                             out: Collector[String]): Unit = { // 触发计算
          val top3 = topNState.get().iterator().toList.sortWith((a1, a2) => a1.count > a2.count).take(3)
          val stringBuilder = new StringBuilder
          stringBuilder.append(s"地区广告点击统计，窗口结束时间 ${timestamp-10L}\n")
          top3.foreach{ac =>
            stringBuilder.append(s"provience:${ac.province}, ad-id: ${ac.adId}, click: ${ac.count}\n")
          }
          out.collect(stringBuilder.toString())
        }
      })

    mainDS.print("ok")
    mainDS.getSideOutput(lateOutput).print("lazy")

    env.execute(getClass.getSimpleName)
  }
}
