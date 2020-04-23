package com.bigdata.flink.item.app

import java.io.{BufferedReader, File, FileInputStream, InputStreamReader}
import java.text.SimpleDateFormat
import java.util.Properties

import com.bigdata.flink.item.bean.url.{ApacheLogEvent, LogSchema}
import com.bigdata.flink.item.transform.url.{CountAgg, TopNHotUrls, WindowResultFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.junit.Test

class HotUrls {

  @Test
  def top3Url(): Unit ={
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
        props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

        val producer = new KafkaProducer[Int, String](props)
        implicit val formats = DefaultFormats
        val topic="apache-log"

        var reader:BufferedReader = null
        try {
          val path = "/Users/huhao/softwares/idea_proj/flink-demo/hot-item/src/main/resources/input/apache.log"
          reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))))
          var line = reader.readLine()
          while (line != null) {
            val strings = line.split(" ")
            val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
            val tmstp = sdf.parse(strings(3)).getTime
            val event = ApacheLogEvent(strings(0), strings(2), tmstp, strings(5), strings(6))
            val partition = math.abs(event.url.hashCode) % 3
            val key = math.abs(event.ip.hashCode)
            val json = write(event)
            val record = new ProducerRecord[Int, String](topic, partition, null, key, json)
            producer.send(record)
            line = reader.readLine()
          }
          producer.flush()
        }finally {
          reader.close()
        }
      }
    })

    val receiveThread = new Thread(new Runnable {
      override def run(): Unit = {
        val props = new Properties()
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092")
        props.put("zookeeper.connect", "hadoop01:2181,hadoop02:2181,hadoop03:2181")
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("group.id", "consumer-group1")
        props.put("auto.offset.reset", "earliest")

        val topic="apache-log"
        val pattern = "^((?!\\.(css|js)$).)*$".r
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

        val kafkaStream = env.addSource(new FlinkKafkaConsumer[ApacheLogEvent](topic, new LogSchema(), props))

        val markedStream = kafkaStream.assignTimestampsAndWatermarks(
          // 运行有1秒延迟
          new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.minutes(1)) {
          override def extractTimestamp(event: ApacheLogEvent): Long = event.eventTime
        })

        val filtedStream = markedStream.filter(event=>pattern.findFirstIn(event.url).nonEmpty)
        val windowedStream = filtedStream.keyBy("url").timeWindow(Time.minutes(10),Time.seconds(5))
        val aggedStream = windowedStream.aggregate(new CountAgg(), new WindowResultFunction())

        val topNStream = aggedStream.keyBy("windowEnd").process(new TopNHotUrls(5))

        topNStream.print()

        env.execute("top 5 url")

      }
    })

    receiveThread.start()
//    sendThread.start()
    receiveThread.join()
//    sendThread.join()

  }





}
