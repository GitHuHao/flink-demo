package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SessionWindowTimeGapExtractor, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration

class Window {

  /**
   * 滚动窗口：无重复
   */
  @Test
  def trumbling(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      )
    )

    dsWithTs.keyBy("id")
      .timeWindow(Time.seconds(2))
      .reduce((w1,w2)=> WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc + w2.vc))
      .print()

    env.execute("trumbling")
  }

  /**
   * 滑动窗口
   * 2> WaterSensor(sensor_2,1607527992000,2)
   * 5> WaterSensor(sensor_1,1607527992050,2)
   * 2> WaterSensor(sensor_2,1607527992000,2)
   * 2> WaterSensor(sensor_2,1607527994000,3)
   * 5> WaterSensor(sensor_1,1607527992050,2)
   * 5> WaterSensor(sensor_1,1607527994050,11)
   * 2> WaterSensor(sensor_2,1607527995550,32)
   * 5> WaterSensor(sensor_1,1607527994050,11)
   * 2> WaterSensor(sensor_2,1607527996000,53)
   * 2> WaterSensor(sensor_2,1607527996000,24)
   */
  @Test
  def sliding(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      )
    )

    dsWithTs.keyBy("id")
      .timeWindow(Time.seconds(2),Time.seconds(1))
      .reduce((w1,w2)=> WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc + w2.vc))
      .print()

    env.execute("trumbling")
  }

  /**
   * session gap 静态窗口
   */
  @Test
  def gapWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))
    dsWithTs.keyBy("id")
      .window(EventTimeSessionWindows.withGap(Time.seconds(1))
      ).reduce((w1,w2)=> WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc + w2.vc))
      .print()
    env.execute("gapWindow")
  }

  /**
   * 动态gap窗口
   * 2> WaterSensor(sensor_2,1607527993000,5)
   * 2> WaterSensor(sensor_2,1607527997000,53)
   * 5> WaterSensor(sensor_1,1607527992050,2)
   * 5> WaterSensor(sensor_1,1607527994050,11)
   */
  @Test
  def gapWindow2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))
    dsWithTs.keyBy("id")
      .window(EventTimeSessionWindows.withDynamicGap(
        new SessionWindowTimeGapExtractor[WaterSensor] {
          override def extract(t: WaterSensor): Long = {
            if(t.id.endsWith("2")){
              2000L
            }else{
              1000L
            }
          }
      })
      ).reduce((w1,w2)=> WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc + w2.vc))
      .print()
    env.execute("gapWindow2")
  }

  @Test
  def globalWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2))
      .withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))
    dsWithTs.keyBy("id")
      .window(GlobalWindows.create())
//      .process() // 收集全部元素到一个窗口，需要自定义trigger触发计算
//      .print()
    env.execute("gapWindow2")
  }

  @Test
  def countWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
    }))

    dsWithTs.keyBy("id")
      .countWindow(2) // 每2个元素，进行一次输出，不足2个的就不输出
      .reduce((w1,w2)=>WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc+w2.vc))
      .print()
    env.execute("countWindow")
  }

  @Test
  def countWindow2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }))

    dsWithTs.keyBy("id")
      .countWindow(2,1) // 每进入一个元素，就抓取最近两个元素进行计算
      .reduce((w1,w2)=>WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc+w2.vc))
      .print()
    env.execute("countWindow2")
  }

  /**
   * 非keyed流使用窗口必须设置并行度为1
   */
  @Test
  def nonKeyWindow(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val seq = Seq( WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))

    dsWithTs.windowAll(TumblingEventTimeWindows.of(Time.seconds(2)))
      .reduce(new ReduceFunction[WaterSensor] {
        override def reduce(value1: WaterSensor, value2: WaterSensor): WaterSensor = {
          println(s"${value1} + ${value2}")
          WaterSensor(value1.id,math.max(value1.ts,value2.ts),value1.vc+value2.vc)
        }
      }).print()

    env.execute("reduce")
  }

}
