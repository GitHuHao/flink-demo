package com.bigdata.flink.model

import java.util.Properties

import com.bigdata.flink.bean.Metric
import com.bigdata.flink.source.MysqlSource
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.TextInputFormat
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write
import org.junit.Test

import scala.util.Random

case class SensorReading(id:String, timestamp:Long, temperature:Double)

class Source {

  val s3File:String = "hdfs://hadoop01:9000/apps/mr/wc/in/1.txt"
  val localFile = CommonSuit.getFile("1.txt")

  @Test
  def readTextFile(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 读取本地文件
    //    val stream:DataStream[String] = env.readTextFile(localFile)
    // 读取s3文件
    val stream:DataStream[String] = env.readTextFile(s3File)
    stream.print()
    env.execute("readTextFile")
  }

  @Test
  def readFile(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val path = new Path(localFile)
    val inputFormat = new TextInputFormat(path)
    // import org.apache.flink.api.scala._
    val stream = env.readFile(inputFormat, localFile)
    stream.print()
    env.execute("readFile")
  }

  /**
   * 启动服务端： nc -l 4000
   * 然后启动监听端 app
   */
  @Test
  def socketTextStream(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[String] = env.socketTextStream("localhost", 4000)
    stream.print()
    env.execute("socketTextStream")
  }

  /**
   * 基于迭代器创建建流
   */
  @Test
  def fromCollection(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ints = Iterator(1, 2, 3, 4)
    val stream = env.fromCollection(ints)
    stream.print("col")
    env.execute("fromCollection")
  }

  /**
   * 直接基于元素创建流
   */
  @Test
  def fromElements(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[Int] = env.fromElements(1, 2, 3)
    stream.print("elem")
    env.execute("fromElements")
  }

  /**
   * 生成序列
   */
  @Test
  def generateSequence(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[Long] = env.generateSequence(1, 4)
    stream.print("gen")
    env.execute("generateSequence")
  }

  @Test
  def kafkaSource(): Unit ={
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop01:9092")
        props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[String, String](props)

        val name = "mem"
        val tmstp = System.currentTimeMillis()
        val tags = Map[String, String]("app" -> "hadoop cluster", "ip" -> "hadoop01")
        val fields = Map[String,Double]("used_percent" -> 90d,"max"->27244873d,"used"->17244873d,"init"->27244873d)
        val metric = new Metric(name,tmstp,tags,fields)

        val topic = "metric_test"
        val partition = null
        val timestamp = null
        val key = null

        implicit val formats: DefaultFormats = DefaultFormats
        val value =write(metric)
        println(value)

        val record = new ProducerRecord[String, String](topic, partition, timestamp, key, value)
        producer.send(record)

        producer.flush();
      }
    })

    val receiveThread = new Thread(new Runnable {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop01:9092")
        props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        props.put("group.id", "metric-group")
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer") //key 反序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        props.put("auto.offset.reset", "latest") //value 反序列化

        val kafkaConsumer = new FlinkKafkaConsumer[String]("metric_test",new SimpleStringSchema(),props)

        val dataStreamSource = env.addSource(kafkaConsumer).setParallelism(1)

        dataStreamSource.print()

        env.execute("kafka data source")
      }
    })

    receiveThread.start()
    sendThread.start()
    receiveThread.join()
    sendThread.join()
  }

  @Test
  def mysqlSource(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("url","jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8")
    props.put("className","com.mysql.jdbc.Driver")
    props.put("user","test")
    props.put("password","test")

    val sql = "select * from student where age>17"
    val sqlStream = env.addSource(new MysqlSource(props, sql))
    sqlStream.print()
    env.execute("mysql data source")
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



}
