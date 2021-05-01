package com.bigdata.flink.source

import java.util.Properties
import com.bigdata.flink.model.{SensorReading, WaterSensor}
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.junit.Test

import java.util.concurrent.TimeUnit
import scala.util.Random

class Source {

  /**
   * 从集合创建流
   */
  @Test
  def fromCollection(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.8),
      SensorReading("sensor_6", 1547718201, 12.8),
      SensorReading("sensor_7", 1547718202, 12.5),
      SensorReading("sensor_7", 1547718205, 38.1)
    ))
    stream.print()
    env.execute("fromCollection")
  }

  @Test
  def fromCollection2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromCollection(List(WaterSensor("ws_001", 1577844001L, 45),
      WaterSensor("ws_002", 1577844015L, 43),
      WaterSensor("ws_003", 1577844020L, 42))
    )
    ds.print()
    env.execute("fromCollection2")
  }

  /**
   * 基于枚举原始创建流（注：元素类型可以不一致）
   */
  @Test
  def fromElements(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从集合创建流
    val stream = env.fromElements(
      SensorReading("sensor_1", 1547718199, 35.8),
      1,
      "AAA",
      SensorReading("sensor_6", 1547718201, 12.8),
      SensorReading("sensor_7", 1547718202, 12.5),
      SensorReading("sensor_7", 1547718205, 38.1)
    )
    stream.print()
    env.execute("fromElements")
  }

  /**
   * 读取本地文件产生流
   */
  @Test
  def readTextFile(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从集合创建流
    val textStream = env.readTextFile(getClass.getClassLoader.getResource("sensor/sensor.txt").getFile)
    val stream = textStream.map{line =>
      val strings = line.split(",")
      SensorReading(strings(0),strings(1).toLong,strings(2).toDouble)
    }
    stream.print()
    env.execute("readTextFile")
  }

  @Test
  def readFile2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.readTextFile(CommonSuit.getFile("wc/1.txt"))
    ds.print("readFile2")
    env.execute("readFile2")
  }

  @Test
  def readFile3(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.readTextFile("hdfs://hadoop01:9000/user/admin/temp/student/month=201706/000000_0")
    ds.print()
    env.execute("readFile3")
  }

  /**
   * 从 socket 创建流
   */
  @Test
  def socketTextStream(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从集合创建流
    val socketStream = env.socketTextStream("localhost", 7000)
    val stream = socketStream.map{line =>
      val strings = line.split(",")
      SensorReading(strings(0),strings(1).toLong,strings(2).toDouble)
    }
    stream.print()

    env.execute("socketTextStream")
  }

  /**
   * 从 从 kafka 消息 创建流
   */
  @Test
  def kafkaStream(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 从集合创建流
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    properties.setProperty("group.id","consumer-group")
    properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
    properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserialization")
    properties.setProperty("auto.offset.reset","latest")

    val kStream = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), properties))

    val stream = kStream.map{line =>
      val strings = line.split(",")
      SensorReading(strings(0),strings(1).toLong,strings(2).toDouble)
    }
    stream.print()

    env.execute("kafkaStream")
  }

  @Test
  def kafkaStream2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.setProperty("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
    props.setProperty("group.id","kafkaStream2")
    props.setProperty("auto.offset.reset","latest")
    val ds = env.addSource(new FlinkKafkaConsumer[String]("senson",new SimpleStringSchema(),props))
    ds.map{line =>
      val strings = line.split(",")
      SensorReading(strings(0),strings(1).toLong,strings(2).toDouble)
    }.print()
    env.execute("kafkaStream2")
  }


  /**
   * 自定义数据源，充当模拟数据
   */
  @Test
  def sourceFunc(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val sourceFunction = new SourceFunction[SensorReading] {
      // 控制是否继续产生数据开关
      var running: Boolean = true

      // 模拟数据收集间隔
      val collectInterval:Long = 1000l

      // 收集数据
      override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        val rand = new Random()

        // 模拟一轮收集操作
        val baseSream = 1.to(10).map(
          i => ("sensor_" + i, 60 + rand.nextGaussian() * 20) // 在 60 基础上添加随机高斯波动
        )

        while(running){ // 循环控制一直产生数据
          val randStream = baseSream.map(
            data => (data._1, data._2 + rand.nextGaussian()) // 在已经有的高斯波动基础上添加更小的高斯波动
          )
          val tmstp = System.currentTimeMillis()
          randStream.foreach(
            data => sourceContext.collect(SensorReading(data._1,tmstp,data._2)) // 发射数据
          )
          Thread.sleep(collectInterval)
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    }

    val mockStream = env.addSource(sourceFunction)

    mockStream.print()

    env.execute("sourceFunc")
  }

  @Test
  def sourceFunc1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(new SourceFunction[SensorReading](){

      var running:Boolean = true
      val random:Random = new Random()

      override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
        val ds = (0 to 10).map(i=> (s"sensor_${i}",60+random.nextGaussian()))
        while(running){
          val randDS = ds.map(data => (data._1,data._2 + random.nextGaussian()))
          val tmstp = System.currentTimeMillis()
          randDS.foreach(
            data => sourceContext.collect(SensorReading(data._1,tmstp,data._2))
          )
          TimeUnit.SECONDS.sleep(1)
        }
      }

      override def cancel(): Unit = {
        running = false
      }
    })

    ds.print()

    env.execute("sourceFunc1")
  }


}
