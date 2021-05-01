package com.bigdata.flink.source

import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{AggregateFunction, ReduceFunction}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration

/**
 * 窗口聚合函数
 * reduce: 进出类型一致，适合常规算术运算：累计、最大、最小，进入一条元素，计算一次，效率高；
 * aggregate：进出类型可以不一致，有中间过渡状态，适用场景比reduce多，进入一条元素，计算一次，效率高；
 * process：进出类型可以不一致，可以有中间状态，适用场景最多，可以混入窗口时间信息，先收集全部元素到集合中，然后使用循环计算，计算效率低，适用场景广。
 */
class Agg {

  @Test
  def reduce(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements( WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))

    dsWithTs.keyBy("id")
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .reduce(new ReduceFunction[WaterSensor] {
        override def reduce(value1: WaterSensor, value2: WaterSensor): WaterSensor = {
          println(s"${value1} + ${value2}")
          WaterSensor(value1.id,math.max(value1.ts,value2.ts),value1.vc+value2.vc)
        }
      }).print()

    env.execute("reduce")
  }

  @Test
  def aggregate(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements( WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))

    dsWithTs.keyBy("id")
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .aggregate(new AggregateFunction[WaterSensor,(String,Long,Int),WaterSensor] {
        override def createAccumulator():(String,Long,Int) = { // 初始化累加器
          println("createAccumulator")
          ("",0L,0)
        }

        override def add(value: WaterSensor, acc:(String,Long,Int)):(String,Long,Int) = { // 加1
          println("add")
          (value.id,value.ts,acc._3+value.vc)
        }

        override def getResult(acc:(String,Long,Int)): WaterSensor = { // 最后提取结果
          println("getResult")
          WaterSensor(acc._1,acc._2,acc._3)
        }

        override def merge(a:(String,Long,Int), b:(String,Long,Int)):(String,Long,Int) = { // 合并
          (a._1,math.max(a._2,b._2),a._3+b._3)
        }
      }).print()

    env.execute("aggregate")
  }

  /**
   * process 窗口聚合函数，先收集，然后在窗口关闭时一次性处理，由于需要存储所有元素，因此占用内存资源比较大，且之前不能进行预计算，因此性能不如reduce和aggregate
   * 2> WaterSensor(sensor_2,1607527993000,5)
   * 5> WaterSensor(sensor_1,1607527992050,2)
   * 5> WaterSensor(sensor_1,1607527994050,11)
   * 2> WaterSensor(sensor_2,1607527995550,29)
   * 2> WaterSensor(sensor_2,1607527997000,24)
   */
  @Test
  def process(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements( WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[WaterSensor](Duration.ofSeconds(2)).withTimestampAssigner(
      new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }
    ))

    dsWithTs.keyBy(_.id) // 注：此处必须以点方式明确key类型，否则后面是有ProcessWindowFunction会报类型问题type mismatch
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .process(new ProcessWindowFunction[WaterSensor,WaterSensor,String,TimeWindow](){
        override def process(key: String, context: Context, elements: Iterable[WaterSensor], out: Collector[WaterSensor]): Unit = {
          var vc:Int = 0
          var ts:Long = 0L
          var id:String = null
          elements.foreach{w =>
            if(w.ts>ts){
              ts = w.ts
            }
            vc+=w.vc
            id = w.id
          }
          out.collect(WaterSensor(id,ts,vc))
        }
      }).print()

    env.execute("reduce")
  }





}
