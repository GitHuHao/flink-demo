package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import java.util.concurrent.TimeUnit

class Timer {

  /**
   * 基于处理时间设置定时任务
   * 注：对应有界流最后几个元素的定时任务不一定被触发。
   * 对于无界流一定被触发。
   */
  @Test
  def processTimeTimer(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.socketTextStream("localhost",9000).map((_,1))
    ds.keyBy(_._1).process(new ProcessFunction[(String,Int),String] {
      override def onTimer(timestamp: Long, ctx: ProcessFunction[(String,Int), String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(s"${timestamp-2000} 定时任务被触发")
      }
      override def processElement(i: (String,Int), ctx: ProcessFunction[(String,Int), String]#Context, collector: Collector[String]): Unit = {
        // 设置处理时间 2秒后触发定时任务
        val tmsp = ctx.timerService().currentProcessingTime() + 2000
        ctx.timerService().registerProcessingTimeTimer(tmsp)
        collector.collect(s"元素${i}设置${tmsp}定时任务")
      }
    }).print()
    env.execute("processFunc")
  }

  /**
   * 基于事件时间设置定时任务
   */
  @Test
  def eventTimeTimer(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(("a", 1607527992000L), ("b", 1607527992000L), ("c", 1607527995000L), ("d", 1607527992000L))
    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long)] {
          override def extractTimestamp(element: (String,Long), recordTimestamp: Long): Long = element._2
        }))
    ds.keyBy(_._1).process(new ProcessFunction[(String,Long),String] {
      override def onTimer(timestamp: Long, ctx: ProcessFunction[(String,Long), String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect(s"${timestamp-2000} 定时任务被触发")
      }
      override def processElement(i: (String,Long), ctx: ProcessFunction[(String,Long), String]#Context, collector: Collector[String]): Unit = {
        // 设置处理时间 2秒后触发定时任务
        val tmsp = ctx.timestamp() + 2000
        ctx.timerService().registerProcessingTimeTimer(tmsp)
        collector.collect(s"元素${i}设置${tmsp}定时任务")
      }
    }).print()
    env.execute("processFunc")
  }

  @Test
  def vcUpAlert(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527970000L, 1),
      WaterSensor("sensor_1", 1607527973000L, 9),
      WaterSensor("sensor_1", 1607527976000L, 8),
      WaterSensor("sensor_1", 1607527980000L, 14),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    val vcUpTag = new OutputTag[String]("vc-up")

    val mainDS = ds.keyBy(_.id)
      .process(new KeyedProcessFunction[String, WaterSensor, WaterSensor] {
        private var lastVCState:ValueState[Int] = _
        private var lastTimerState:ValueState[Long] = _

        override def open(parameters: Configuration): Unit = {
          lastVCState = getRuntimeContext.getState(new ValueStateDescriptor("last-vc", classOf[Int], Int.MinValue))
          lastTimerState = getRuntimeContext.getState(new ValueStateDescriptor("last-timer", classOf[Long], Long.MinValue))
        }

        override def processElement(i: WaterSensor, ctx: KeyedProcessFunction[String, WaterSensor, WaterSensor]#Context,
                                    collector: Collector[WaterSensor]): Unit = {
          val lastVC = lastVCState.value()
          val lastTimer = lastTimerState.value()
          if(i.vc > lastVC){
            if(lastTimer==Long.MinValue){
              val tmstp = ctx.timestamp() + 5000L
              ctx.timerService().registerEventTimeTimer(tmstp)
              lastTimerState.update(tmstp)
            }
          }else{
            if(lastTimer != Long.MinValue){
              ctx.timerService().deleteEventTimeTimer(lastTimer)
            }
            lastTimerState.update(Long.MinValue)
          }
          lastVCState.update(i.vc)
          collector.collect(i)
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, WaterSensor, WaterSensor]#OnTimerContext,
                             out: Collector[WaterSensor]): Unit = {
          val lastVC = lastVCState.value()
          val lastTimer = lastTimerState.value()
          ctx.output(vcUpTag,s"${ctx.getCurrentKey}  ${lastTimer-5000L}(${lastVC})~${lastTimer}(↑)")
          lastTimerState.update(Long.MinValue)
        }

      })

    mainDS.getSideOutput(vcUpTag).print("up")

    env.execute("SensorTemperatrueUpAlert")
  }




}
