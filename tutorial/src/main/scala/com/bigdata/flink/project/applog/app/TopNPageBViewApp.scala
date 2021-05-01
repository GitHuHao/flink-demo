package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.{ApacheLog, PageCount}
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import scala.collection.convert.ImplicitConversions.`iterator asScala`

/**
 * 滑动窗口 每5秒钟统计最近1小时中页面访问数top3
 */
object TopNPageBViewApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val watermarkStrategy = WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(60)).withTimestampAssigner(
      new SerializableTimestampAssigner[ApacheLog] {
      override def extractTimestamp(element: ApacheLog, recordTimestamp: Long): Long = {
        element.eventTime
      }
    })

    env.readTextFile(CommonSuit.getFile("applog/apache.log"))
      .map{line =>
        val sdf = new SimpleDateFormat("MM/dd/yyyy:HH:mm:ss")
        val arr = line.split("\\s+")
        ApacheLog(arr(0),sdf.parse(arr(3)).getTime,arr(5),arr(6))
      }.assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(_.url)
      .window(SlidingEventTimeWindows.of(Time.minutes(60),Time.seconds(5)))
      .aggregate(
        new AggregateFunction[ApacheLog,Long,Long] {  // url 访问次数累计
          override def createAccumulator(): Long = 0L

          override def add(value: ApacheLog, accumulator: Long): Long = accumulator + 1L

          override def getResult(accumulator: Long): Long = accumulator

          override def merge(a: Long, b: Long): Long = a + b
        },
        new ProcessWindowFunction[Long,PageCount,String,TimeWindow] { // 窗口收敛时，在url访问累计数基础上，带上窗口信息，输出
          override def process(key: String, context: Context, elements: Iterable[Long],
                               out: Collector[PageCount]): Unit = {
            out.collect(PageCount(key,elements.head,context.window.getEnd))
          }
        }
      ).keyBy(_.windowEnd) // 收集相同窗口
       .process(new KeyedProcessFunction[Long,PageCount,String] { // 同一个窗口内取访问数 topN,输出

         private var timerState:ValueState[Long] = _
         private var topNState:ListState[PageCount] = _

         override def open(parameters: Configuration): Unit = {
           timerState = getRuntimeContext.getState(new ValueStateDescriptor("timer",classOf[Long]))
           topNState = getRuntimeContext.getListState(new ListStateDescriptor[PageCount]("topN-list",classOf[PageCount]))
         }

         override def processElement(i: PageCount, context: KeyedProcessFunction[Long, PageCount, String]#Context,
                                     collector: Collector[String]): Unit = {
           topNState.add(i)
           if(timerState.value()==null){
             context.timerService().registerEventTimeTimer(i.windowEnd + 10L) // 收集到首个元素后倒计时10秒，然后进行topN统计
             timerState.update(i.windowEnd)
           }
         }

         override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageCount, String]#OnTimerContext,
                              out: Collector[String]): Unit = {
           val top3 = topNState.get().iterator().toList.sortWith((p1, p2) => p1.count > p2.count).take(3)
           topNState.clear()
           timerState.clear()
           val stringBuilder = new StringBuilder()
           stringBuilder.append(s"页面访问量top3统计，窗口结束时间：${timestamp-10}\n")
           top3.foreach{pc =>
             stringBuilder.append(s"${pc.url} ${pc.count}\n")
           }
           out.collect(stringBuilder.toString())
         }

       }).print()
    env.execute(getClass.getSimpleName)
  }

}
