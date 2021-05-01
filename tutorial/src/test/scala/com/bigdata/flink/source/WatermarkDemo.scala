package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import com.bigdata.flink.watermark.{MyPeriodWatermark, MyPunctuatedWatermark}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration
import java.util.concurrent.TimeUnit


class WatermarkDemo {

  /**
   * 升序水印
   * WaterSensor(sensor_1,1607527992050,2)
   * WaterSensor(sensor_2,1607527993000,5)
   * WaterSensor(sensor_1,1607527994050,11)
   * WaterSensor(sensor_2,1607527995550,29)
   * WaterSensor(sensor_2,1607527997000,24)
   */
  @Test
  def forMonotonousTimestamps(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val seq = Seq(
      ("0001", 1585015882000L),	 // 数据eventTime为：2020-03-24 10:11:22  数据waterMark为  2020-03-24 10:11:12
      ("0001", 1585015885000L),	 // 数据eventTime为：2020-03-24 10:11:25  数据waterMark为  2020-03-24 10:11:15
      ("0001", 1585015888000L),	 // 数据eventTime为：2020-03-24 10:11:28  数据waterMark为  2020-03-24 10:11:18
      ("0001", 1585015890000L),	 // 数据eventTime为：2020-03-24 10:11:30  数据waterMark为  2020-03-24 10:11:20
      ("0001", 1585015891000L),	 // 数据eventTime为：2020-03-24 10:11:31  数据waterMark为  2020-03-24 10:11:21
      ("0001", 1585015895000L),	 // 数据eventTime为：2020-03-24 10:11:35  数据waterMark为  2020-03-24 10:11:25
      ("0001", 1585015898000L),	 // 数据eventTime为：2020-03-24 10:11:38  数据waterMark为  2020-03-24 10:11:28
      ("0001", 1585015900000L),	 // 数据eventTime为：2020-03-24 10:11:40  数据waterMark为  2020-03-24 10:11:30  触发第一条到第三条数据计算，数据包前不包后，不会计算2020-03-24 10:11:30 这条数据
      ("0001", 1585015911000L),	 // 数据eventTime为：2020-03-24 10:11:51  数据waterMark为  2020-03-24 10:11:41  触发2020-03-24 10:11:20到2020-03-24 10:11:28时间段的额数据计算，数据包前不包后，不会触发2020-03-24 10:11:30这条数据的计算
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps[(String,Long)]()
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long)] {
        override def extractTimestamp(element: (String,Long), recordTimestamp: Long): Long = element._2
      }))

    dsWithTs.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      //      .reduce((w1,w2)=> WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc+w2.vc))
      .process(new ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,Long)], out: Collector[String]): Unit = {
          val w = context.window
          out.collect(s"[${w.getStart}, ${w.getEnd}) ${elements.size} ${elements.map(_._2).mkString(" | ")}")
        }
      })
      .print()

    env.execute("forMonotonousTimestamps")
  }

  /**
   * forBoundedOutOfOrderness 处理乱序数据
   * 水印10s，即允许出现10秒延迟
   * 窗口10s，即窗口界定为 [0,10),[10,20),[20,30),...
   * 步骤：
   * 1）依据事件时间换算出水印，保存水印递增特性（出现减小时，顺延前面的）；
   * 2）确定依据水印，确定水印之前最贴近窗口（左闭右开），检查该元素进入时是否会触发窗口关闭（窗口能收集到数据就可以关闭）；
   * 3）窗口一旦关闭，之后匹配上的元素，就视为迟到数据，被丢弃，或采用额外逻辑补偿计算；
   * 4）有界流最后一个元素进入时，会触发所有窗口关闭
   * send (0001,1585015948000) send (0001,1585015945000) send (0001,1585015947000) send (0001,1585015950000) send (0001,1585015960000) [1585015940000, 1585015950000) 3 1585015948000 | 1585015945000 | 1585015947000
   * send (0001,1585015949000) [1585015950000, 1585015960000) 1 1585015950000
   * [1585015960000, 1585015970000) 1 1585015960000
   *
   *
   */
  @Test
  def forBoundedOutOfOrderness(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val seq = Seq(
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015945000L),	// 数据eventTime为：2020-03-24 10:12:25  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015950000L),	// 数据eventTime为：2020-03-24 10:12:30  数据waterMark为  2020-03-24 10:12:20
      ("0001", 1585015960000L), // 数据eventTime为：2020-03-24 10:12:40  数据waterMark为  2020-03-24 10:12:30  触发计算 waterMark > eventTime 并且窗口内有数据，
      // 触发 2020-03-24 10:12:28到2020-03-24 10:12:27 这三条数据的计算，数据包前不包后，不会触发2020-03-24 10:12:30 这条数据的计算
      ("0001", 1585015949000L),	// 数据eventTime为：2020-03-24 10:12:29  数据waterMark为  2020-03-24 10:12:30  迟到太多的数据，flink直接丢弃，
      // 可以设置flink将这些迟到太多的数据保存起来，便于排查问题 )
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(new SerializableTimestampAssigner[(String,Long)] {
        override def extractTimestamp(element: (String,Long), recordTimestamp: Long): Long = element._2
      }))

    dsWithTs.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//      .reduce((w1,w2)=> WaterSensor(w1.id,math.max(w1.ts,w2.ts),w1.vc+w2.vc))
      .process(new ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,Long)], out: Collector[String]): Unit = {
          val w = context.window
          out.collect(s"[${w.getStart}, ${w.getEnd}) ${elements.size} ${elements.map(_._2).mkString(" | ")}")
        }
      })
      .print()

    env.execute("forBoundedOutOfOrderness")
  }

  /**
   * 自定义周期性水印，每隔200ms周期性发送一次
   *
   */
  @Test
  def periodWatermark(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val seq = Seq(
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015945000L),	// 数据eventTime为：2020-03-24 10:12:25  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015950000L),	// 数据eventTime为：2020-03-24 10:12:30  数据waterMark为  2020-03-24 10:12:20
      ("0001", 1585015960000L), // 数据eventTime为：2020-03-24 10:12:40  数据waterMark为  2020-03-24 10:12:30  触发计算 waterMark > eventTime 并且窗口内有数据，
      // 触发 2020-03-24 10:12:28到2020-03-24 10:12:27 这三条数据的计算，数据包前不包后，不会触发2020-03-24 10:12:30 这条数据的计算
      ("0001", 1585015949000L),	// 数据eventTime为：2020-03-24 10:12:29  数据waterMark为  2020-03-24 10:12:30  迟到太多的数据，flink直接丢弃，
      // 可以设置flink将这些迟到太多的数据保存起来，便于排查问题 )
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(new MyPeriodWatermark(Duration.ofSeconds(10)).withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long)] {
        override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
      })
    )

    dsWithTs.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,Long)], out: Collector[String]): Unit = {
          val w = context.window
          out.collect(s"[${w.getStart}, ${w.getEnd}) ${elements.size} ${elements.map(_._2).mkString(" | ")}")
        }
      })
      .print()

    env.execute("forBoundedOutOfOrderness")
  }

  /**
   * 自定义间歇水印， 每来一个元素，生成一次水印
   */
  @Test
  def punctuatedWatermark(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val seq = Seq(
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015945000L),	// 数据eventTime为：2020-03-24 10:12:25  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015950000L),	// 数据eventTime为：2020-03-24 10:12:30  数据waterMark为  2020-03-24 10:12:20
      ("0001", 1585015960000L), // 数据eventTime为：2020-03-24 10:12:40  数据waterMark为  2020-03-24 10:12:30  触发计算 waterMark > eventTime 并且窗口内有数据，
      // 触发 2020-03-24 10:12:28到2020-03-24 10:12:27 这三条数据的计算，数据包前不包后，不会触发2020-03-24 10:12:30 这条数据的计算
      ("0001", 1585015949000L),	// 数据eventTime为：2020-03-24 10:12:29  数据waterMark为  2020-03-24 10:12:30  迟到太多的数据，flink直接丢弃，
      // 可以设置flink将这些迟到太多的数据保存起来，便于排查问题 )
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(new MyPunctuatedWatermark(Duration.ofSeconds(10)).withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long)] {
        override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
      })
    )

    dsWithTs.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .process(new ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,Long)], out: Collector[String]): Unit = {
          val w = context.window
          out.collect(s"[${w.getStart}, ${w.getEnd}) ${elements.size} ${elements.map(_._2).mkString(" | ")}")
        }
      })
      .print()

    env.execute("forBoundedOutOfOrderness")
  }

  /**
   * 水印触发窗口统计，但不立即关闭窗口，而是等到水印增大latness时关闭，关闭后窗口不在接收迟到数据了
   * 注： allowedLateness 只能用在event time上
   */
  @Test
  def allowedLateness(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val seq = Seq(
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015945000L),	// 数据eventTime为：2020-03-24 10:12:25  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015950000L),	// 数据eventTime为：2020-03-24 10:12:30  数据waterMark为  2020-03-24 10:12:20
      ("0001", 1585015960000L), // 数据eventTime为：2020-03-24 10:12:40  数据waterMark为  2020-03-24 10:12:30  触发计算 waterMark > eventTime 并且窗口内有数据，
      // 触发 2020-03-24 10:12:28到2020-03-24 10:12:27 这三条数据的计算，数据包前不包后，不会触发2020-03-24 10:12:30 这条数据的计算
      ("0001", 1585015961000L), // 数据eventTime为：2020-03-24 10:12:41  数据waterMark为  2020-03-24 10:12:31
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:31 水印只增大1秒 < latness，10:12:30统计的窗口仍能接收数据
      ("0001", 1585015962000L), // 数据eventTime为：2020-03-24 10:12:42  数据waterMark为  2020-03-24 10:12:32
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:32  水印增大了2秒 不小于 latness，10:12:30统计的窗口仍能接收数据不在接受数据
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(
      new SerializableTimestampAssigner[(String, Long)] {
        override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
      })
    )

    dsWithTs.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(2))
      .process(new ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,Long)], out: Collector[String]): Unit = {
          val w = context.window
          out.collect(s"[${w.getStart}, ${w.getEnd}) ${elements.size} ${elements.map(_._2).mkString(" | ")}")
        }
      })
      .print()

    env.execute("forBoundedOutOfOrderness")
  }

  /**
   * 处理迟到数据：
   * 1）forBoundedOutOfOrderness 乱序水印 在窗口未触发前(水印抵达窗口边界时触发，左闭右开)，允许有乱序数据
   * 2）allowedLateness 窗口触发后，不立即关闭，允许水印增长到一定程度，在此期间可以继续窗口收集迟到数据，水印增长latness后，窗口关闭，不在接受数据
   * 3）sideOutputLateData 传入侧输出标签，flink在执行process函数时，自动将迟到，本应该被忽略数据，通过侧输出流传出，通过自定义逻辑整合到所属窗口
   */
  @Test
  def sideOutputForLateness(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val seq = Seq(
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015945000L),	// 数据eventTime为：2020-03-24 10:12:25  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:18
      ("0001", 1585015950000L),	// 数据eventTime为：2020-03-24 10:12:30  数据waterMark为  2020-03-24 10:12:20
      ("0001", 1585015960000L), // 数据eventTime为：2020-03-24 10:12:40  数据waterMark为  2020-03-24 10:12:30  触发计算 waterMark > eventTime 并且窗口内有数据，
      // 触发 2020-03-24 10:12:28到2020-03-24 10:12:27 这三条数据的计算，数据包前不包后，不会触发2020-03-24 10:12:30 这条数据的计算
      ("0001", 1585015961000L), // 数据eventTime为：2020-03-24 10:12:41  数据waterMark为  2020-03-24 10:12:31
      ("0001", 1585015947000L),	// 数据eventTime为：2020-03-24 10:12:27  数据waterMark为  2020-03-24 10:12:31 水印只增大1秒 < latness，10:12:30统计的窗口仍能接收数据
      ("0001", 1585015962000L), // 数据eventTime为：2020-03-24 10:12:42  数据waterMark为  2020-03-24 10:12:32
      ("0001", 1585015948000L),	// 数据eventTime为：2020-03-24 10:12:28  数据waterMark为  2020-03-24 10:12:32  水印增大了2秒 不小于 latness，10:12:30统计的窗口仍能接收数据不在接受数据
    )

    val ds = env.addSource(new StreamSourceMock[(String,Long)](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
      .withTimestampAssigner(
        new SerializableTimestampAssigner[(String, Long)] {
          override def extractTimestamp(element: (String, Long), recordTimestamp: Long): Long = element._2
        })
    )

    val latenessTag = new OutputTag[(String, Long)]("lateness-output")

    val mainDS = dsWithTs.keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .allowedLateness(Time.seconds(2))
      .sideOutputLateData(latenessTag) // 传入一个侧输出标签，自动收集迟到数据
      .process(new ProcessWindowFunction[(String,Long),String,String,TimeWindow] {
        override def process(key: String, context: Context, elements: Iterable[(String,Long)], out: Collector[String]): Unit = {
          val w = context.window
          out.collect(s"[${w.getStart}, ${w.getEnd}) ${elements.size} ${elements.map(_._2).mkString(" | ")}")
        }
      })

    mainDS.print("ok")
    mainDS.getSideOutput(latenessTag).print("lateness")

    env.execute("forBoundedOutOfOrderness")
  }



}
