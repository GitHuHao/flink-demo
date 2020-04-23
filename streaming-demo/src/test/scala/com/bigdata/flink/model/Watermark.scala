package com.bigdata.flink.model

import java.io.ObjectOutputStream
import java.net.ServerSocket
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

/**
 * 1.watermatk 是一种衡量EventTime进展的机制，它是数据本身的一个隐藏属性，数据本身携带对应watermark
 * 2.wakermark 用于处理乱序事件，正确处理乱序事件，通常用watermark + window 机制实现
 * 3.乱序产生原因：网络、背压；
 */
class Watermark {

  @Test
  def socketServer(): Unit = {
    val socket = new ServerSocket(9000)
    val clientProxy = socket.accept()
    val sendStream = new ObjectOutputStream(clientProxy.getOutputStream)

    for (i <- 0 until 20) {
      val tmstp = System.currentTimeMillis()
      for (j <- 0 to i) {
        val msg = s"${tmstp} a${j}\n"
        sendStream.writeUTF(msg)
      }
      sendStream.flush()
      Thread.sleep(1000)
    }
  }

  @Test
  def re(): Unit = {
    val pattern = "\\d+".r
    val maybeMatch = pattern.findFirstMatchIn("��z1583911165173 a0")
    val value = maybeMatch.getOrElse("0")
    println(value)
  }

  @Test
  def tumblingWindowWithoutDelay(): Unit = {
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val socket = new ServerSocket(9000)
        val clientProxy = socket.accept()
        val sendStream = new ObjectOutputStream(clientProxy.getOutputStream)
        // 每输出一批休眠1秒
        for (i <- 0 until 20) {
          val tmstp = System.currentTimeMillis()
          for (j <- 0 to i) {
            val msg = s"${tmstp} a${j}\n"
            sendStream.writeUTF(msg)
          }
          sendStream.flush()
          Thread.sleep(1000)
        }
      }
    })

    val streamingThread = new Thread(new Runnable {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val stream = env.socketTextStream("localhost", 9000, '\n', 3)
        val keyedStream = stream.assignTimestampsAndWatermarks(
          // 设置延迟为 0 秒，到点执行
          new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
            override def extractTimestamp(line: String): Long = {
              val pattern = "\\d+".r // 正则提取 消息中的eventtime
              val sysTime = pattern.findFirstMatchIn(line).getOrElse("0").toString.toLong
              sysTime
            }
          }
        ).map(line => (line.split("\\s+")(1), 1)).keyBy(0)
        val windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3)))
        windowedStream.reduce((t1, t2) => (t1._1, t1._2 + t2._2)).print()
        env.execute("TumblingEventTimeWindows")
      }
    })

    streamingThread.start()
    sendThread.start()

    streamingThread.join()
    sendThread.join()
  }

  @Test
  def tumblingWindowWithDelay(): Unit = {
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val socket = new ServerSocket(9000)
        val clientSocket = socket.accept()
        val sendStream = new ObjectOutputStream(clientSocket.getOutputStream)
        // 每输出一批休眠1秒
        for (i <- 0 until 20) {
          val tmstp = System.currentTimeMillis()
          for (j <- 0 to i) {
            val msg = s"${tmstp} a${j}\n"
            sendStream.writeUTF(msg)
          }
          sendStream.flush()
          Thread.sleep(1000)
        }
      }
    })

    val streamingThread = new Thread(new Runnable {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val stream = env.socketTextStream("localhost", 9000, '\n', 3)
        val keyedStream = stream.assignTimestampsAndWatermarks(
          // 在指定滚动窗口基础上延迟1秒再计算，通过水印 + 窗口，解决数据乱序问题，延迟时间要保证能最大程度兼容乱序
          new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(1000)) {
            override def extractTimestamp(line: String): Long = {
              val pattern = "\\d+".r // 正则提取 消息中的eventtime
              val sysTime = pattern.findFirstMatchIn(line).getOrElse("0").toString.toLong
              sysTime
            }
          }
        ).map(line => (line.split("\\s+")(1), 1)).keyBy(0)
        val windowedStream = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(3)))
        windowedStream.reduce((t1, t2) => (t1._1, t1._2 + t2._2)).print()
        env.execute("TumblingEventTimeWindows")
      }
    })

    streamingThread.start()
    sendThread.start()

    streamingThread.join()
    sendThread.join()
  }

  /**
   * 滑动窗口不设置延迟
   */
  @Test
  def slidingWindowWithoutDelay(): Unit ={
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val socket = new ServerSocket(9000)
        val clientSocket = socket.accept()
        val sendStream = new ObjectOutputStream(clientSocket.getOutputStream)
        // 每输出一批休眠1秒
        for (i <- 0 until 20) {
          val tmstp = System.currentTimeMillis()
          for (j <- 0 to i) {
            val msg = s"${tmstp} a${j}\n"
            sendStream.writeUTF(msg)
          }
          sendStream.flush()
          Thread.sleep(1000)
        }
      }
    })

    val streamingThread = new Thread(new Runnable {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val stream = env.socketTextStream("localhost", 9000, '\n', 3)
        val keyedStream = stream.assignTimestampsAndWatermarks(
          // 在指定滚动窗口基础上延迟1秒再计算，通过水印 + 窗口，解决数据乱序问题，延迟时间要保证能最大程度兼容乱序
          new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
            override def extractTimestamp(line: String): Long = {
              val pattern = "\\d+".r // 正则提取 消息中的eventtime
              val sysTime = pattern.findFirstMatchIn(line).getOrElse("0").toString.toLong
              sysTime
            }
          }
        ).map(line => (line.split("\\s+")(1), 1)).keyBy(0)
        val windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(3),Time.seconds(2)))
        windowedStream.reduce((t1, t2) => (t1._1, t1._2 + t2._2)).print()
        env.execute("SlidingEventTimeWindows")
      }
    })

    streamingThread.start()
    sendThread.start()

    streamingThread.join()
    sendThread.join()
  }

  /**
   * 滑动窗口 + 水印 + 延迟策略
   * 需要注意点：
   * 1.socket 输出需要带 \n 换行分隔符
   * 2.必须执行 env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   */
  @Test
  def slidingWindowWithDelay(): Unit ={
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val serverSocket = new ServerSocket(9000)
        val clientProxy = serverSocket.accept()
        val outputStream = new ObjectOutputStream(clientProxy.getOutputStream)
        for(i<- 0 until 20){
          val tmstp = System.currentTimeMillis()
          for(j<- 0 to i){
            val msg = s"${tmstp} a${j}\n"
            outputStream.writeUTF(msg)
          }
          outputStream.flush()
          Thread.sleep(1000)
        }
      }
    })

    val streamThread = new Thread(new Runnable {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val socketStream = env.socketTextStream("localhost", 9000, '\n', 3)
        val extractedStream = socketStream.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(1000)) {
            override def extractTimestamp(line: String): Long = {
              val pattern = "\\d+".r
              val tmstp = pattern.findFirstMatchIn(line).getOrElse("0").toString.toLong
              tmstp
            }
          }
        )
        val keyedStream = extractedStream.map(x => (x.split("\\s+")(1), 1)).keyBy(0)
        val windowStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(3), Time.seconds(2)))
        windowStream.reduce((t1,t2)=>(t1._1,t1._2+t2._2)).print()
        env.execute("slidingWindowWithDelay")
      }
    })

    sendThread.start()
    streamThread.start()

    sendThread.join()
    streamThread.join()
  }

  /**
   * EventTimeSessionWindows.withGap
   * 接收元素时间间隔超过指定标准，就拆分窗口执行计算
   *
   */
  @Test
  def sessionGapWindow(): Unit ={
    val sendThread = new Thread(new Runnable {
      override def run(): Unit = {
        val serverSocket = new ServerSocket(9000)
        val clientProxy = serverSocket.accept()
        val outputStream = new ObjectOutputStream(clientProxy.getOutputStream)
        for(i<- 0 until 20){
          val tmstp = System.currentTimeMillis()
          for(j <- 0 to i){
             val msg = s"${tmstp} a${j}\n"
            outputStream.writeUTF(msg)
          }
          outputStream.flush()
          val duration = if(i%4==0) 5000 else 0
          Thread.sleep(duration)
        }
      }
    })

    val streamThread = new Thread(new Runnable {
      override def run(): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        val socketStream = env.socketTextStream("localhost", 9000)
        val extractStream = socketStream.assignTimestampsAndWatermarks(
          new BoundedOutOfOrdernessTimestampExtractor[String](Time.milliseconds(0)) {
            override def extractTimestamp(line: String): Long = {
              val pattern = "\\d+".r
              val tmstp = pattern.findFirstMatchIn(line).getOrElse("0").toString.toLong
              tmstp
            }
          }
        )
        val sessionStream = extractStream.map(line=> (line.split("\\s+")(1),1))
          .keyBy(0)
          .window(EventTimeSessionWindows.withGap(Time.seconds(5)))
        sessionStream.reduce((t1,t2)=>(t1._1,t1._2+t2._2)).print()
        env.execute("EventTimeSessionWindows")
      }
    })

    sendThread.start()
    streamThread.start()

    sendThread.join()
    streamThread.join()
  }

}
