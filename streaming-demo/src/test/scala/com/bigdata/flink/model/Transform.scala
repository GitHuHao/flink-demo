package com.bigdata.flink.model

import com.bigdata.flink.suit.CommonSuit
import com.bigdata.flink.transform.{FilterFunc, FlatMapFunc, FoldFunc, JoinFunc, KeyFunc, MapFunc, ReduceFunc, RichFilterFunc, RichFlatMapFunc, RichMapFunc}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.ExecutionEnvironment
import org.junit.Test
import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.scala.extensions._
import org.apache.flink.streaming.api.datastream.DataStreamUtils

import scala.collection.JavaConverters.asScalaIteratorConverter
case class Point(x: Double, y: Double)

class Transform {

  val localFile = CommonSuit.getFile("1.txt")
  val s3File = "hdfs://hadoop01:9000/apps/mr/wc/in/1.txt"

  /**
   * 一对一转换操作
   * def map[R: TypeInformation](fun: T => R): DataStream[R]
   */
  @Test
  def map(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1, 3).map(x => x * 2).print()
    env.execute("map")
  }

  /**
   * 一对多
   * def flatMap[R: TypeInformation](fun: T => TraversableOnce[R]): DataStream[R]
   */
  @Test
  def flatMap(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.readTextFile(localFile).flatMap(_.split("\\s+")).print()
    env.execute("flatMap")
  }

  /**
   * 过滤出符合条件元素
   * def filter(fun: T => Boolean): DataStream[T]
   */
  @Test
  def filter(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.generateSequence(1, 3).filter(_ == 1).print()
    env.execute("filter")
  }

  /**
   * def connect[T2](dataStream: DataStream[T2]): ConnectedStreams[T, T2]
   *  1.连接两个流，可以是不同数据类型，被连接之后，各流仍保持各自的独立性
   *  2.先迭代完第一个，再迭代第二个
   * Connect与 Union 区别：
   * 1． Union之前两个流的类型必须是一样，Connect可以不一样，在之后的coMap中再去调整成为一样的。
   * 2. Connect只能操作两个流，Union可以操作多个。
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

  /**
   * 分隔流同时，使用select 提取拆分过后的流
   * def split(fun: T => TraversableOnce[String]): SplitStream[T]
   */
  @Test
  def split(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val splitStream: SplitStream[String] = env.readTextFile(localFile).flatMap(_.split("\\s+"))
      .split(elem =>
        (elem.startsWith("a")) match {
          case true => List("a") // 贴标签
          case false => List("other")
        }
      )
    val stream1: DataStream[String] = splitStream.select("a")
    val stream2: DataStream[String] = splitStream.select("other")
    val stream3: DataStream[String] = splitStream.select("a","other") // 选取多个
    stream1.print("a")
    stream2.print("other")
    stream3.print("many")
    env.execute("split")
  }

  /**
   * 从给定流中抽取指定 ds2
   * def iterate[R](stepFunction: DataStream[T] => (DataStream[T], DataStream[R]),
   * maxWaitTimeMillis:Long = 0) : DataStream[R]
   */
  @Test
  def iterate(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.generateSequence(0,10)
    val splited:DataStream[Long] = stream.iterate{it =>
      val ds1 = it.filter(_ % 2 == 0)
      val ds2 = it.filter(_ % 2 == 1)
      (ds1,ds2)
    }
    splited.print()
    env.execute("iterate")
  }

  /**
   * 合并流
   * def union(dataStreams: DataStream[T]*): DataStream[T]
   * 输出时仍能区分元素来源于那个流
   */
  @Test
  def union(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream1: DataStream[String] = env.readTextFile(localFile)
    val stream2: DataStream[String] = env.readTextFile(localFile)
    val unionStream: DataStream[String] = stream1.union(stream2)
    unionStream.print()
    env.execute()
  }

  /**
   * 基于第二个字段分区，从打印效果看，分区字段相同的元素由同一线程打印输出
   * def keyBy(fields: Int*): KeyedStream[T, JavaTuple]
   */
  @Test
  def keyBy(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile(localFile)
    val keyedStream: KeyedStream[Array[String], Tuple] = stream.map(_.split("\\s+")).keyBy(1)
    keyedStream.map(arr => arr.mkString(",")).print()
    env.execute("keyBy")
  }

  /**
   * 对 KeyedStream 执行 规约操作
   * def reduce(fun: (T, T) => T): DataStream[T]
   */
  @Test
  def reduce(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile(localFile)
    stream.map { line =>
      val data = line.split("\\s+")
      (data(1), 1)
    }.keyBy(0)
      .reduce { (t1, t2) => (t1._1, t1._2 + t2._2) }
      .print()
    env.execute("reduce")
  }

  /**
   * 折叠操作执行wordcount
   * def fold[R: TypeInformation](initialValue: R)(fun: (R,T) => R): DataStream[R]
   */
  @Test
  def fold(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile(localFile)
    stream.map { line =>
      val strings = line.split("\\s+")
      (strings(1), 1)
    }.keyBy(0).fold(("", 0l))((begin, item) => (item._1, +begin._2 + item._2))
      .print()
    env.execute("fold")
  }

  /**
   * 对 keyedStream 执行max min sum 操作
   */
  @Test
  def aggregtions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile(localFile)
    val keyedStream = stream.map { line =>
      val strings = line.split("\\s+")
      (strings(1), strings(0).length)
    }.keyBy(0)
    keyedStream.max(1).print("max")
    keyedStream.maxBy(1).print("maxBy")

    keyedStream.min(1).print("min")
    keyedStream.minBy(1).print("minBy")

    keyedStream.sum(1).print("sum")
    env.execute()
  }

  /**
   * 针对Dataset 增强功能
   * import org.apache.flink.api.scala.extensions._
   */
  @Test
  def extensions(): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(Point(1, 2), Point(3, 4), Point(5, 6))
    ds.filterWith {
      case Point(x, _) => x > 1
    }.groupingBy{
      case Point(x,_) => x
    }.reduceWith {
      case (Point(x1, y1), (Point(x2, y2))) => Point(x1 + y1, x2 + y2)
    }.mapWith {
      case Point(x, y) => (x, y)
    }.flatMapWith {
      case (x, y) => Seq("x" -> x, "y" -> y)
    }.print()
  }

  /**
   * 设置缓冲超时，防止网络通信，导致数据传输不稳定
   */
  @Test
  def setBufferTimeout(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //    env.setBufferTimeout(1000)
    val stream = env.generateSequence(1, 5)
    val bufferedStream = stream.map{x=>
      if(x==3){
        Thread.sleep(1500)
      }
      x
    }.setBufferTimeout(1000)
    bufferedStream.print()
    env.execute("buffered stream")
  }

  @Test
  def dataStreamUtils(): Unit ={
    // import scala.collection.JavaConverters.asScalaIteratorConverter java 集合 与  scala 集合相互转换
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile(localFile)
    val wcStream:DataStream[(String,Int)] = stream.flatMap(_.split("\\s+")).map((_, 1)).keyBy(0).reduce((t1,t2)=>(t1._1,t1._2+t2._2))
    val iter:Iterator[(String,Int)] = DataStreamUtils.collect(wcStream.javaStream).asScala
    iter.foreach(println(_))
  }

  @Test
  def mapFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val func = new MapFunc()
    env.fromElements("a", "b", "c")
      .map(func)
      .print()
  }

  /**
   * RichXXXFunction 可以获取生命周期相关函数 open() close()，并可以通过上下文context()获取并行度、任务名称、state状态。
   * open()方法是rich function的初始化方法，当一个算子例如map或者filter被调用之前open()会被调用。
   * close()方法是生命周期中的最后一个调用的方法，做一些清理工作。
   * getRuntimeContext()方法提供了函数的RuntimeContext的一些信息，例如函数执行的并行度，任务的名字，以及state状态
   */
  @Test
  def richMapFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val richMap = new RichMapFunc()
    env.fromElements("a", "b", "c")
      .map(richMap)
      .print()
  }

  @Test
  def flatMapFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val func = new FlatMapFunc()
    env.fromElements("a b c","d,e,f")
      .flatMap(func)
      .print()
  }

  @Test
  def richFlatMapFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val func = new RichFlatMapFunc()
    env.fromElements("a b c","d,e,f")
      .flatMap(func)
      .print()
  }

  @Test
  def filterFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val func = new FilterFunc()
    env.fromElements("ipad","apple","island")
      .filter(func)
      .print()
  }

  @Test
  def richFilterFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val func = new RichFilterFunc()
    env.fromElements("ipad","apple","island")
      .filter(func)
      .print()
  }

  @Test
  def keyedFunc(): Unit ={
    // 将字符按奇偶分区，然后每个元素依次输出对应分区最小值，生成了 keyedStream
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val func = new KeyFunc()
    env.fromCollection("a,b,c,d,e,f".split(","))
      .keyBy(func)
      .min(0)
      .print()
    env.execute("keyed by")
  }

  @Test
  def reduceFunc(): Unit ={
    // 按字符奇偶分区，然后拼接起来
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyedFunc = new KeyFunc()
    val reduceFunc = new ReduceFunc()
    env.fromCollection("a,b,c,d,e,f".split(","))
      .keyBy(keyedFunc)
      .reduce(reduceFunc)
      .print()
    env.execute()
  }

  @Test
  def foldFunc(): Unit ={
    // 按字符奇偶分区，然后拼接起来
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val keyedFunc = new KeyFunc()
    val foldFunc = new FoldFunc()

    env.fromCollection("a,b,c,d,e,f".split(","))
      .keyBy(keyedFunc)
      .fold("",foldFunc)
      .print()

    env.execute("fold function")
  }

  @Test
  def joinFunc(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val stream1 = env.fromElements(("A", 1), ("B", 2), ("C", 3))
    val stream2 = env.fromElements(("B", 20), ("C", 30),("D",40))
    val func = new JoinFunc()
    stream1.join(stream2)
      .where(0) // 第一个流 第一个字段 等于 第二个流 第一个字段时，进行连接，然后执行join 拼接
      .equalTo(0)
      .apply(func)
      .print()
  }

  @Test
  def project(): Unit ={
    val env = ExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements(
      ("a",1,11,111),
      ("b",2,22,222),
      ("c",3,33,333)
    )
    // 此版本中暂未找到project函数
    stream.map(t=>(t._2,t._3))
      .print()
  }

}
