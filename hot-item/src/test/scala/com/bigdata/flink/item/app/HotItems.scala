package com.bigdata.flink.item.app

import java.io.{BufferedReader, FileInputStream, InputStreamReader}
import java.util.Properties

import com.bigdata.flink.item.bean.behavior.{BehaviorSchema, UserBehavior}
import com.bigdata.flink.item.transform.behavior.{CountAgg, TopNHotItems, WindowResultFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.junit.Test

class HotItems {

  /**
   * 得到了带有水印的数据流
   */
  @Test
  def addWaterMark(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 声明使用事件时间作为流处理时间
    env.setParallelism(1)

    val path = this.getClass.getClassLoader.getResource("input/UserBehavior.csv").getPath

    val behaviorStream = env.readTextFile(path)
      .map{line =>
        val fields = line.split(",")
        UserBehavior(
          fields(0).toLong,
          fields(1).toLong,
          fields(2).toInt,
          fields(3),
          fields(4).toLong
        )
      }.assignAscendingTimestamps(_.timestamp*1000)
    // 数据已经被处理过，所以直接使用升序时间戳生成水印
    // 真实业务中日志通常是乱序的，所以应当使用 BoundedOutOfOrdernessTimestampExtractor 乱序事件戳抽取水印

    behaviorStream.print()

    env.execute("Hot Items Job")
  }

  /**
   * 采用滑动窗口(每隔5m，按itemId统计过去60m商品点击pv)
   */
  @Test
  def slidingAgg(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 声明使用事件时间作为流处理时间
    env.setParallelism(1)

    val path = this.getClass.getClassLoader.getResource("input/UserBehavior.csv").getPath

    val behaviorStream = env.readTextFile(path)
      .map{line =>
        val fields = line.split(",")
        UserBehavior(
          fields(0).toLong,
          fields(1).toLong,
          fields(2).toInt,
          fields(3),
          fields(4).toLong
        )
      }.assignAscendingTimestamps(_.timestamp*1000)
    // 数据已经被处理过，所以直接使用升序时间戳生成水印
    // 真实业务中日志通常是乱序的，所以应当使用 BoundedOutOfOrdernessTimestampExtractor 乱序事件戳抽取水印

    behaviorStream.filter(_.behavior=="pv") // 过滤出点击事件（即 pv）
        .keyBy("itemId") // 记忆itemId进行分区
        .timeWindow(Time.minutes(60),Time.minutes(5)) // 创建滑动窗口，窗口大小为60m,滑动间隔5m
        .aggregate(new CountAgg(),new WindowResultFunction()) // CountAgg 累计器时刻都在运行，符合滑动窗口条件事件触发 窗口计算输出 WindowResultFunction
        .print()

    env.execute("Hot Items Job")
  }

  /**
   * 每隔5分钟，统计过去一小时，点击事件(pv)
   */
  @Test
  def pvTop3(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 声明使用事件时间作为流处理时间
    env.setParallelism(1)

    val path = this.getClass.getClassLoader.getResource("input/UserBehavior.csv").getPath

    val behaviorStream = env.readTextFile(path)
      .map{line =>
        val fields = line.split(",")
        UserBehavior(
          fields(0).toLong,
          fields(1).toLong,
          fields(2).toInt,
          fields(3),
          fields(4).toLong
        )
      }.assignAscendingTimestamps(_.timestamp*1000)
    // 数据已经被处理过，所以直接使用升序时间戳生成水印
    // 真实业务中日志通常是乱序的，所以应当使用 BoundedOutOfOrdernessTimestampExtractor 乱序事件戳抽取水印

    behaviorStream.filter(_.behavior=="pv") // 过滤出点击事件（即 pv）
      .keyBy("itemId") // 记忆itemId进行分区
      .timeWindow(Time.minutes(60),Time.minutes(5)) // 创建滑动窗口，窗口大小为60m,滑动间隔5m
      .aggregate(new CountAgg(),new WindowResultFunction()) // CountAgg 累计器时刻都在运行，符合滑动窗口条件事件触发 窗口计算输出 WindowResultFunction
      .keyBy("windowEnd")
      .process(new TopNHotItems(3))
      .print()

    // aggregate 先聚合，然后执行窗口统计，避免直接在apply中聚合，导致数据量过大

    env.execute("Hot Items Job")
  }

  @Test
  def simulateByKafka(): Unit ={
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop01:9092")
        props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[Int, String](props)
        val path = this.getClass.getClassLoader.getResource("input/UserBehavior.csv").getPath
        val bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
        val topic = "hot-items"
        implicit val formats: DefaultFormats = DefaultFormats

        var line:String = null
        line = bufferedReader.readLine()

        while(line != null){
          val fields = line.split(",")
          val behavior = UserBehavior(
            fields(0).toLong,
            fields(1).toLong,
            fields(2).toInt,
            fields(3),
            fields(4).toLong * 1000
          )

          val json = write(behavior)

          val partition = (behavior.itemId % 3).toInt
          val record = new ProducerRecord[Int, String](topic, partition, null, partition, json)
          producer.send(record)
          line = bufferedReader.readLine()
        }
        producer.flush();
      }
    })

    val receiveThread = new Thread(new Runnable {
      override def run(): Unit = {
        val props = new Properties()

        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
        props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("group.id", "consumer-group2")
        props.put("auto.offset.reset", "earliest")

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) // 设置事件时间为 EventTime
        env.setParallelism(1) // 设置全局并行度

        val kafkaStream = env.addSource(new FlinkKafkaConsumer[UserBehavior]("hot-items", new BehaviorSchema(), props))

        val markedStream = kafkaStream.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.minutes(1)) {
            override def extractTimestamp(behavior: UserBehavior): Long = behavior.timestamp
          }
        ) // 抽取时间，此处使用的是 BoundedOutOfOrdernessTimestampExtractor（可以容许一定时间段内乱序）

        val pvStream = markedStream.filter(_.behavior=="pv") // 过滤出pv事件

        val windowStream = pvStream.keyBy("itemId") // 能被keyBy操作的case class，需要满足一定条件
          .timeWindow(Time.minutes(60),Time.minutes(5))

        val aggedStream = windowStream.aggregate(new CountAgg(),new WindowResultFunction()) // 没接收一个元素，就执行一次聚合操作，满足窗口切分条件时，就拆分窗口

        val topNStream = aggedStream.keyBy("windowEnd") // 对窗口内统计的 ItemViewCount 对象，取点击数top-N 操作
          .process(new TopNHotItems(3))

        topNStream.print()

        env.execute("Hot items job")
      }
    })

    receiveThread.start()
//    sendThread.start()
    receiveThread.join()
//    sendThread.join()
  }







}
