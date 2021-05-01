package com.bigdata.flink.source

import com.bigdata.flink.model.SensorReading
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test
import collection.JavaConversions._

class Transform {

  /**
   * 一对一 元素转换
   */
  @Test
  def map(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = env.readTextFile(getClass.getClassLoader.getResource("sensor/sensor.txt").getPath)

    val stream = textStream.map { line =>
      val strings = line.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }

    stream.print()
    env.execute("map")
  }

  @Test
  def map1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(1, 2, 3, 4)
    ds.map(i => i * i).print()
    env.execute("map1")
  }

  @Test
  def richMap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val ds = env.fromElements(1, 2, 3, 4)
    // open close 属于生命周期函数，执行次数与并行度有关，map方法每处理一个函数执行一次
    ds.map(new RichMapFunction[Int, Int] {
      override def open(parameters: Configuration): Unit = {
        println("open ...")
      }

      override def close(): Unit = {
        println("close ...")
      }

      override def map(value: Int): Int = value * value

    }).print()
    env.execute("richMap")
  }

  /**
   * flatMap 一个元素拆成多个元素
   */
  @Test
  def flatMap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val socketStream = env.socketTextStream("localhost", 7000)
    val stream = socketStream.flatMap(_.split(","))
      .map((_, 1))
      .keyBy(0)
      .sum(1)

    stream.print()
    env.execute("map")
  }

  @Test
  def flatMap1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.readTextFile(CommonSuit.getFile("wc/1.txt"))
      .flatMap(_.split("\\s+")) // 由一个元素转换为一个集合，并且将集合每一个元素作为新的输出
      .print()
    env.execute("flatMap1")
  }

  /**
   * 过滤出符合条件指定元素
   */
  @Test
  def filter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = env.readTextFile(getClass.getClassLoader.getResource("sensor/sensor.txt").getPath)

    val stream = textStream.map { line =>
      val strings = line.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }.filter(_.timestamp % 2 == 0)

    stream.print()
    env.execute("filter")
  }

  @Test
  def filter1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(1, 2, 3, 4)
    ds.filter(_ % 2 == 0).print()
    env.execute("filter1")
  }

  /**
   * keyBy key 相同元素交给同一个线程处理
   * 默认基于hashcode 进行重分区
   */
  @Test
  def keyed(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = env.readTextFile(getClass.getClassLoader.getResource("sensor/sensor.txt").getPath)

    val stream = textStream.map { line =>
      val strings = line.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }
      //      .keyBy(0) // 下标、字段名称 、自定义键选择器
      //        .keyBy("id")
      .keyBy(new KeySelector[SensorReading, String] {
        override def getKey(in: SensorReading): String = in.id
      })
    stream.print()
    env.execute("keyed")
  }

  @Test
  def keyBy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.readTextFile(CommonSuit.getFile("wc/1.txt"))
      .flatMap(_.split("\\s+"))
      .map((_, 1))
      .keyBy(0) // tuple 取序号
      .print()
    env.execute("keyBy")
  }

  @Test
  def keyBy2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val textStream = env.readTextFile(getClass.getClassLoader.getResource("sensor/sensor.txt").getPath)
    textStream.map { line =>
      val strings = line.split(",")
      SensorReading(strings(0), strings(1).toLong, strings(2).toDouble)
    }.keyBy("id") // 对象取字段
      .print()
    env.execute("keyBy2")
  }

  @Test
  def keyBy3(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(0 to 10:_*)
      .keyBy(new KeySelector[Int,String]() { // 对于非kv数据，也能使用keyBy进行分组
        override def getKey(value: Int): String = {
          if(value % 2 == 0){
            "jishu"
          }else{
            "oushu"
          }
        }
      }).print()
    env.execute("keyBy3")
  }

  /**
   * 将给定元素随机打乱
   */
  @Test
  def shuffle(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromElements(0 to 10:_*).shuffle.print()
    env.execute("shuffle")
  }

  // 合并两个流，两个流数据结构不要求完全一致
  @Test
  def connect1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(0,1,2,3)
    val ds2 = env.fromElements('a','b','c','d')
    val ds3 = ds1.connect(ds2)
    ds3.map(i=>i*i,s=>s*2).print()
    env.execute("split")
  }


  /**
   * def connect[T2](dataStream: DataStream[T2]): ConnectedStreams[T, T2]
   * 1.连接两个流，可以是不同数据类型，被连接之后，各流仍保持各自的独立性
   * 2.先迭代完第一个，再迭代第二个
   */
  @Test
  def connect(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[Long] = env.generateSequence(0, 3)
    val stream2: DataStream[String] = env.fromElements("java", "scala")
    val connectedStream = stream1.connect(stream2)
    connectedStream.map(elem1 => elem1 + 10, elem2 => elem2.toUpperCase).print()
    env.execute("connect")
  }

  // union 合并多个结构相同的流
  @Test
  def union(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromElements(1,2,3,4)
    val ds2 = env.fromElements(10,20,30,40)
    val ds3 = env.fromElements(100,200,300,400)
    val ds = ds1.union(ds2,ds3)
    ds.print()
    env.execute("union")
  }

  // 求max,min,sum 每过来一条元素，就会更新对于分组的相关信息（最大值、最小值、累计和）
  @Test
  def max(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val keyedDS = env.fromElements(0 to 10: _*).keyBy(i => if (i % 2 == 0) ">" else "<")
    keyedDS.max(0).print("max")
    keyedDS.min(0).print("min")
    keyedDS.sum(0).print("sum")
    env.execute("max")
  }

  @Test
  def maxBy(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val ds = env.fromElements(0,1,1,2,3,4)
    ds.keyBy(i=> if(i%2==0)"<" else ">").maxBy(0).print("maxBy")
    ds.keyBy(i=> if(i%2==0)"<" else ">").minBy(0).print("minBy")
    env.execute("maxBy")
  }

  // 规约操作，reduce输入和输出数据类型必须一致
  @Test
  def reduce(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromElements(0,1,1,2,3,4)
    .keyBy(i=> if(i%2==0)">" else "<")
    .reduce(_+_)
    .print("sum")

    env.fromElements("0,1,2,3,4".split(","):_*)
      .keyBy(i=> if(i.toInt %2==0)">" else "<")
      .reduce((i1,i2) => s"${i1},${i2}")
      .print("merge")

    env.execute("reduce")
  }

  // process可以执行结构变换、过滤、状态计算、输出
  // ValueState、ListState、SetState、MapState
  // AggregateState、FoldState、ReduceState
  @Test
  def process(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.fromElements(0,1,1,2,3,4)
      .keyBy(i => if(i%2==0)">" else "<")
      .process(new ProcessFunction[Int,String]{

        private var listState:ListState[String] = _

        override def open(parameters: Configuration): Unit = {
          val value = new ListStateDescriptor("",classOf[String])
          listState = getRuntimeContext.getListState(value)
        }

        override def processElement(i: Int, context: ProcessFunction[Int, String]#Context,
                                    collector: Collector[String]): Unit = {
          listState.add(s"${i}")
          listState.get().iterator()
          collector.collect(listState.get().mkString(","))
        }

        override def close(): Unit = {
          if(listState!=null){
            listState.clear()
          }
        }

      }).print()

    env.execute("process")
  }


  /**
   * 重分区算子 keyBy、rebalance、resale、shuffle
   * keyBy 指定分区
   * shuffle 乱序随机分区
   * rebalance 出现数据倾斜是，重新平衡、顺序将元素分发给下游节点，需要经过网络
   * rescale 同rebalance一样，重新平衡分布数据，效率比rebalance高，基于管道实现
   */



}
