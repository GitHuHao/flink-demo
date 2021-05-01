package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.JoinFunction
import org.apache.flink.api.common.state.{MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.co.{BroadcastProcessFunction, CoProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration


/**
 * Flink SQL 基于process 实现
 * process 可以实现:
 * 1）map、flapMap转换结构；
 * 2）filter 过滤；
 * 3）数据输出；
 * 4）状态存储；相关操作；
 *
 * ProcessFunction 处理keyed流和non-keyed流；
 * KeyedProcessFunction：处理keyed流；
 *
 *
 */
class Process {

  /**
   * 结构转换
   */
  @Test
  def process(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq("hello hello hi",
      "python hello hi",
      "jack hello hi",
      "jack python hi",
      "hello hello java",
    )

    val ds = env.addSource(new StreamSourceMock[String](seq, false))

    ds.process(new ProcessFunction[String, String] {
      override def processElement(i: String, context: ProcessFunction[String, String]#Context,
                                  collector: Collector[String]): Unit = {
        collector.collect(i.toUpperCase())
      }
    }).print()

    env.execute("keyProcess")
  }

  /**
   * 处理kv结构，进行wordcount操作
   */
  @Test
  def keyProcess(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq("hello hello hi",
      "python hello hi",
      "jack hello hi",
      "jack python hi",
      "hello hello java",
    )

    val ds = env.addSource(new StreamSourceMock[String](seq, false))

    ds.flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(_._1)
      .process(new ProcessFunction[(String, Int), (String, Int)] {
        private var valueState: ValueState[Int] = _

        override def open(parameters: Configuration): Unit = {
          val valueStateDesc = new ValueStateDescriptor[Int]("wordcount", classOf[Int])
          valueState = getRuntimeContext.getState(valueStateDesc)
        }

        override def processElement(i: (String, Int), context: ProcessFunction[(String, Int), (String, Int)]#Context,
                                    collector: Collector[(String, Int)]): Unit = {
          val newValue = i._2 + valueState.value()
          valueState.update(newValue)
          collector.collect((i._1, newValue))
        }
      }).print()

    env.execute("keyProcess")
  }

  /**
   * 解决乱序流匹配问题，且可以解决长时间不上问题
   */
  @Test
  def coProcess(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val orders = Seq(("a", 1), ("b", 2), ("c", 3), ("d", 4))
    val payments = Seq(("b", 200), ("c", 300), ("a", 100), ("d", 400))

    val ds1 = env.addSource(new StreamSourceMock[(String, Int)](orders, false))
    val ds2 = env.addSource(new StreamSourceMock[(String, Int)](payments, false))

    val unionDS = ds1.connect(ds2)

    unionDS.keyBy(_._1, _._1)
      .process(new CoProcessFunction[(String, Int), (String, Int), String] {
        private var orderMap: MapState[String, Int] = _
        private var paymentMap: MapState[String, Int] = _

        override def open(parameters: Configuration): Unit = {
          orderMap = getRuntimeContext.getMapState(new MapStateDescriptor("orders", classOf[String], classOf[Int]))
          paymentMap = getRuntimeContext.getMapState(new MapStateDescriptor("payments", classOf[String], classOf[Int]))
        }

        override def processElement1(in1: (String, Int), context: CoProcessFunction[(String, Int), (String, Int), String]#Context,
                                     collector: Collector[String]): Unit = {
          val key = in1._1
          if (paymentMap.contains(key)) {
            val paymentInfo = paymentMap.get(key)
            collector.collect(s"1: ${in1} -> (${key},${paymentInfo})")
            paymentMap.remove(key)
          } else {
            orderMap.put(in1._1, in1._2)
          }
        }

        override def processElement2(in2: (String, Int), context: CoProcessFunction[(String, Int), (String, Int), String]#Context,
                                     collector: Collector[String]): Unit = {
          val key = in2._1
          if (orderMap.contains(key)) {
            val orderInfo = orderMap.get(key)
            collector.collect(s"2: (${key},${orderInfo}) -> (${in2})")
            // TODO 注意这里需要删除已经匹配的key
            orderMap.remove(key)
          } else {
            // 未匹配上的就暂存到状态
            paymentMap.put(in2._1, in2._2)
          }
        }
      }).print()

    env.execute("coProcess")
  }

  /**
   * 双流窗口join，必须拉窗口，只有在一个窗口内的才能成功关联，使用局限性非常大
   * 2> (b,1607527992000) -> (b,1607527993000)  刚好都落在 [1607527992000,1607527994000) 窗口内
   * a [1607527992000,1607527994000) 一个在内，一个在外，无法join
   * c [1607527992000,1607527994000) 1607527995000  一个在内，一个在外，无法join
   * d [1607527992000,1607527994000) 1607527996000 一个在内，一个在外，无法join
   */
  @Test
  def joinFunc(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val orders = Seq(("a", 1607527992000L), ("b", 1607527992000L), ("c", 1607527995000L), ("d", 1607527992000L))
    val payments = Seq(("b", 1607527993000L), ("c", 1607527992000L), ("a", 1607527994000L), ("d", 1607527996000L))

    val ds1 = env.addSource(new StreamSourceMock[(String, Long)](orders, false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(
        new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
        }
      ))

    val ds2 = env.addSource(new StreamSourceMock[(String, Long)](payments, false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[(String, Long)] {
            override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
          }
        ))

    ds1.join(ds2).where(_._1).equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .apply(new JoinFunction[(String,Long),(String,Long),String] {
        override def join(first: (String, Long), second: (String, Long)): String = {
          s"${first} -> ${second}"
        }
      }).print()

    env.execute("joinFunc")
  }

  /**
   * 广播状态：将低流量流广播，其余正常业务流与其连接，在BroadcastProcessFunction中，通过关广播流引入配置变更信息，动态调整业务处理逻辑
   * 举例：flink cds 配置表指导binlog动态分流
   */
  @Test
  def broadcastProcessFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.socketTextStream("localhost",9001)
    val ds2 = env.socketTextStream("localhost",9002)

    val broadcastStateDesc = new MapStateDescriptor("config-ds", classOf[String], classOf[String])
    val broadcastDS = ds1.broadcast(new MapStateDescriptor("config-ds", classOf[String], classOf[String]))

    val connectDS = ds2.connect(broadcastDS)

    connectDS.process(new BroadcastProcessFunction[String,String,String]{
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                  collector: Collector[String]): Unit = {
        val config = readOnlyContext.getBroadcastState(broadcastStateDesc)
        if(config.contains("switch") && config.get("switch").equals(in1)){
          collector.collect(in1)
        }
      }

      override def processBroadcastElement(in2: String, ctx: BroadcastProcessFunction[String, String, String]#Context,
                                           collector: Collector[String]): Unit = {
        val config = ctx.getBroadcastState(broadcastStateDesc)
        config.put("switch",in2)
        collector.collect(s"switch config to ${in2}")
      }
    }).print()

    env.execute("broadcastState")
  }

  // TODO keyBy 广播
  def keyedBroadcastProcessFunction(): Unit ={

  }

  // TODO 窗口处理函数
  def processWindowFunction(): Unit ={

  }

  // TODO 全窗口函数
  def ProcessAllWindowFunction(): Unit ={

  }


}
