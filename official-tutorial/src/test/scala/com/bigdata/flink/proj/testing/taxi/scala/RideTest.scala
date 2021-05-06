package com.bigdata.flink.proj.testing.taxi.scala

import com.bigdata.flink.proj.common.func.StreamSourceMock
import com.bigdata.flink.proj.taxi.bean.EnrichRide
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.{FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.state.{MapStateDescriptor, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, RichCoFlatMapFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.GeoUtils
import org.apache.flink.util.Collector
import org.joda.time.{Interval, Minutes}
import org.junit.Test

import java.time.Duration
import java.util.Date
import scala.collection.convert.ImplicitConversions.`iterator asScala`

class RideTest {

  @Test
  def map(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .map(new EnrichRide(_)) // 辅助构造器创建对象需要new,主构造器构造对象不需要new
      .print()
    env.execute("map")
  }

  // flatMap默认用法还拿到集合，然后以元素为单位向外分发
  @Test
  def flatMap1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements("hello,hi", "jack,hello")
      .flatMap(_.split("\\,"))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
      .print()
    env.execute("map")
  }

  // flatMap实质定义的是，输入一个元素，允许多次输出元素，因此输入未必一定是集合
  @Test
  def flatMap2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .flatMap(new FlatMapFunction[TaxiRide, EnrichRide]() {
        override def flatMap(r: TaxiRide, out: Collector[EnrichRide]): Unit = {
          if (GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat)) {
            out.collect(new EnrichRide(r))
          }
        }
      }) // 辅助构造器创建对象需要new,主构造器构造对象不需要new
      .print()
    env.execute("map")
  }

  @Test
  def keyBy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .flatMap(new FlatMapFunction[TaxiRide, (Int, Minutes)]() {
        override def flatMap(r: TaxiRide, out: Collector[(Int, Minutes)]): Unit = {
          val ride = new EnrichRide(r)
          val interval = new Interval(ride.startTime.toEpochMilli, ride.endTime.toEpochMilli)
          val minutes = interval.toDuration.toStandardMinutes
          out.collect((ride.startCell, minutes))
        }
      }) // 辅助构造器创建对象需要new,主构造器构造对象不需要new
      .keyBy(0)
      .maxBy(1)
      .print()
    env.execute("keyBy")
  }

  @Test
  def duplicate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new TaxiRideGenerator)
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
      .flatMap(new RichFlatMapFunction[TaxiRide, EnrichRide]() {

        private lazy val existedState = getRuntimeContext.getState(
          new ValueStateDescriptor("existed-state", classOf[Boolean]))

        override def flatMap(r: TaxiRide, out: Collector[EnrichRide]): Unit = {
          if (GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat)) {
            val existed = existedState.value()
            if (existed == null) {
              existedState.update(existed)
              out.collect(new EnrichRide(r))
            }
          }
        }
      })
      .print()
    env.execute("keyBy")
  }

  /**
   * 注：ListState，BroadcastState 尽管属于算子状态，但只能在 CheckpointedFunction 中使用，其余诸如 RichXxFunction中一律需要使用键控状态
   * flatMap1 与 flatMap2 是竞争关系，
   * 不借助广播变量的话，两个流必须keyBy，才能将配置和数据攒到一起
   */
  @Test
  def connect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ignoreConfig = env.socketTextStream("localhost", 9000).keyBy(x => x)
    val dataDs = env.socketTextStream("localhost", 9001).keyBy(x => x)
    // 借助Keyby 将配置和数据攒到一块
    ignoreConfig.connect(dataDs).flatMap(new RichCoFlatMapFunction[String, String, String]() {
      private lazy val ignoreState = getRuntimeContext.getMapState(
        new MapStateDescriptor("ignore-set", classOf[String], classOf[String]))

      override def flatMap1(in1: String, collector: Collector[String]): Unit = {
        ignoreState.put(in1, null)
      }

      override def flatMap2(in2: String, collector: Collector[String]): Unit = {
        if (!ignoreState.contains(in2)) {
          collector.collect(in2)
        }
      }
    }).print()

    env.execute("connect")
  }

  @Test
  def connectBroadcast(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ignoreConfig = env.socketTextStream("localhost", 9000)
    val dataDs = env.socketTextStream("localhost", 9001)

    // ListState、BroadcastState 作为算子状态出现，避免 两个流都需要keyby
    val broadcastStateDesc = new MapStateDescriptor("ignore-state", classOf[String], classOf[String])
    val ignoreDS = ignoreConfig.broadcast(broadcastStateDesc)

    // 所有数据处理节点都获取一份广播数据
    dataDs.connect(ignoreDS).process(new BroadcastProcessFunction[String,String,String]() {
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                  collector: Collector[String]): Unit = {
        val ignores  = readOnlyContext.getBroadcastState(broadcastStateDesc)
        if(!ignores.contains(in1)){
          collector.collect(in1)
        }
      }

      override def processBroadcastElement(in2: String, context: BroadcastProcessFunction[String, String, String]#Context,
                                           collector: Collector[String]): Unit = {
        val ignores = context.getBroadcastState(broadcastStateDesc)
        ignores.put(in2,null)
      }
    }).print()

    env.execute("connect")
  }
  
  @Test
  def reduce(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 1),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527994000L, 3),
      ("sensor_1", 1607527994050L, 11),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))

    ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
        new SerializableTimestampAssigner[(String,Long,Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
        })
    ).keyBy(_._1)
      .timeWindow(Time.seconds(2))
      .reduce((t1,t2) => (t1._1,Long.MinValue,t1._3 + t2._3), // 增量计算
        new WindowFunction[(String,Long,Int),(String,Long,Int),String,TimeWindow](){
          override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long, Int)],
                             out: Collector[(String, Long, Int)]): Unit = {
            val tuple = input.iterator.next()
            out.collect((tuple._1,window.getStart,tuple._3)) // 裹上窗口时间
          }
        }
      ).print()

    env.execute("reduce")
  }

  @Test
  def processWindowFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(("sensor_1", 1607527992000L, 1),
      ("sensor_1", 1607527992050L, 1),
      ("sensor_2", 1607527992000L, 2),
      ("sensor_2", 1607527994000L, 3),
      ("sensor_1", 1607527994050L, 11),
      ("sensor_2", 1607527995500L, 5),
      ("sensor_2", 1607527995550L, 24),
      ("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))

    ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(1)).withTimestampAssigner(
        new SerializableTimestampAssigner[(String,Long,Int)] {
          override def extractTimestamp(element: (String, Long, Int), recordTimestamp: Long): Long = element._2
        })
    ).keyBy(_._1)
      .timeWindow(Time.seconds(2))
      .process(new ProcessWindowFunction[(String,Long,Int),(String,Long,Int),String,TimeWindow](){ // 先憋住，然后一次性处理
        override def process(key: String, context: Context, elements: Iterable[(String, Long, Int)],
                             out: Collector[(String, Long, Int)]): Unit = {
          val head = elements.head
          val result = elements.map(_._3).sum
          out.collect((head._1,context.window.getStart,result))
        }
      }).print()

    env.execute("reduce")
  }



}
