package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.{IterativeCondition, SimpleCondition}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

import java.time.Duration
import java.util
import scala.collection.convert.ImplicitConversions.`iterator asScala`


class CEPDemo {

  /**
   * 单例模式：只匹配一次
   * CEP.pattern(ds,pattern)
   * 过滤出sensor_1的流
   *
   * 4> {first=[WaterSensor(sensor_1,1607527996000,22)]}
   * 3> {first=[WaterSensor(sensor_1,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20)]}
   */
  @Test
  def single(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })

    val cepDS = CEP.pattern(ds,pattern) //
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("single")
  }

  /**
   * 循环模式
   * 固定次数：
   * pattern.times(2) 两次出现
   * pattern.times(2, 3) 出现2到2次
   *
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   *
   * 一次或多次：pattern.oneOrMore
   * 两次及两次以上：pattern.timesOrMore(2)
   */
  @Test
  def times(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_2", 1607527993000L, 22),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    // 首尾是 sensor_1，中间可以穿插其他非sensor_1的时间
    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      }).times(2)

    val cepDS = CEP.pattern(ds,pattern) // 出现两次
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("times")
  }

  /**
   * 循环条件
   * 一次及以上 CEP.pattern(ds,pattern.oneOrMore)
   * 5> {first=[WaterSensor(sensor_1,1607527994000,21)]}
   * 7> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 6> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 3> {first=[WaterSensor(sensor_1,1607527992000,20)]}
   * 4> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   * 8> {first=[WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def oneOrMore(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.oneOrMore) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("oneOrMore")
  }

  /**
   * 循环条件：指定次数及以上。
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 3> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def timesOrMore(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.timesOrMore(2)) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

  /**
   * 循环时间 times(2) timesOrMore(2) onceOrMore 之后跟 consecutive() 强调是紧随的
   * 2> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def singleConsecutive(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_2", 1607527993000L, 22),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 循环事件是严格连续的
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).times(2)
      .consecutive() //

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("singleConsecutive")
  }

  /**
   * 循环事件 times(2) onceOrMore timesOrMore 最后使用allowCombinations 表明允许穿插其他事件
   * 7> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 6> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527996000,22)]}
   * 5> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   */
  @Test
  def allowCombinations(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_2", 1607527993000L, 22),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 首尾是sensor_1，中间允许包括sensor_1在内任意事件
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).times(2)
      .allowCombinations() //

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("singleConsecutive")
  }

  /**
   * 不开启greedy
   * 3> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,22)], second=[WaterSensor(sensor_2,1607527994000,21)]}
   * 4> {first=[WaterSensor(sensor_1,1607527993000,22)], second=[WaterSensor(sensor_2,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20)], second=[WaterSensor(sensor_1,1607527993000,22)]}
   *
   * 开启greedy
   * 3> {first=[WaterSensor(sensor_1,1607527993000,22)], second=[WaterSensor(sensor_2,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,22)], second=[WaterSensor(sensor_2,1607527994000,21)]}
   *
   * greedy 贪婪模式常配合 弹性循环条件使用 onceOrMore timesOrMore(2) times(2,3)
   *
   * 弹性循环条件 .greedy 其他条件
   * 当某条件既符合 弹性循环条件 又符合 其他条件，优先按贪婪模式 撑死 弹性条件（即按最多元素个数匹配），然后往前拨动直到又出现匹配项
   * 注：贪婪模式结果一般比正常匹配要少，不能用在模式组上（子模式）。
   *
   */
  @Test
  def greedy(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527993000L, 22),
      WaterSensor("sensor_2", 1607527994000L, 21))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 首尾是sensor_1，中间允许包括sensor_1在内任意事件
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).times(1,2)
      .greedy
      .next("second").where(new SimpleCondition[WaterSensor]() {
        override def filter(t: WaterSensor): Boolean = t.vc >=21
      })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("singleConsecutive")
  }

  /**
   * 6> (first:WaterSensor(sensor_1,1607527993000,22),second:WaterSensor(sensor_2,1607527994000,21))
   * 5> (first:WaterSensor(sensor_1,1607527992000,20),WaterSensor(sensor_1,1607527993000,22),second:WaterSensor(sensor_2,1607527994000,21))
   */
  @Test
  def patternSelectFunction(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527993000L, 22),
      WaterSensor("sensor_2", 1607527994000L, 21))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 首尾是sensor_1，中间允许包括sensor_1在内任意事件
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).times(1,2)
      .greedy
      .next("second").where(new SimpleCondition[WaterSensor]() {
      override def filter(t: WaterSensor): Boolean = t.vc >=21
    })

    val cepDS = CEP.pattern(ds,pattern)

    val splitDS = cepDS.select(new PatternSelectFunction[WaterSensor,(String,String)](){
      override def select(map: util.Map[String, util.List[WaterSensor]]): (String,String) = {
        val firstStr = s"first:${map.get("first").iterator().mkString(",")}"
        val secondStr = s"second:${map.get("second").iterator().mkString(",")}"
        (firstStr,secondStr)
      }
    })

    splitDS.print()

    env.execute("singleConsecutive")
  }

  /**
   * 可选模式：
   * 即前面的条件可以没有，或出现指定次数
   */
  @Test
  def optional(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527993000L, 22),
      WaterSensor("sensor_2", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527996000L, 23))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 出现2次sensor_1 或0次sensor_1，一定有 sensor_2
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).optional
      .next("second").where(new SimpleCondition[WaterSensor]() {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
      })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("optional")
  }

  /**
   * IterativeCondition 父类
   * SimpleCondition、OrCondition、NotCondition、AndCondition、RichIterativeCondition 子类
   * 4> {first=[WaterSensor(sensor_1,1607527994000,21)]}
   * 5> {first=[WaterSensor(sensor_1,1607527996000,22)]}
   * 3> {first=[WaterSensor(sensor_1,1607527992000,20)]}
   */
  @Test
  def iterativeCondition(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first")
      .where(new IterativeCondition[WaterSensor](){ //
        override def filter(t: WaterSensor, context: IterativeCondition.Context[WaterSensor]): Boolean = {
          //          context.getEventsForPattern("")
          t.id.equals("sensor_1")
        }
      })
    val cepDS = CEP.pattern(ds,pattern) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

  /**
   * 组合条件 and or
   * 5> {first=[WaterSensor(sensor_2,1607527999000,24)]}
   * 4> {first=[WaterSensor(sensor_1,1607527992000,20)]}
   */
  @Test
  def or(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor](){ //
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1") && t.vc < 21
      }).or(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2") && t.vc > 22
    })

    val cepDS = CEP.pattern(ds,pattern) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("or")
  }

  /**
   * 默认相当于and 条件
   * 4> {first=[WaterSensor(sensor_1,1607527992000,20)]}
   */
  @Test
  def and(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor](){ //
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      }).where(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.vc<21
    })

    val cepDS = CEP.pattern(ds,pattern) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("and")
  }

  /**
   * until 终止条件：标记匹配结束条件（不包含此结束条件），需要与循环条件 .timesOrMore(2) .oneOrMore 一起使用
   * 1> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def until(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    // 直到出现sensor_2停止
    val pattern = Pattern.begin("first")
      .where(new SimpleCondition[WaterSensor](){ //
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
      .timesOrMore(2)
//      .oneOrMore
      .until(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
      })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("and")
  }

  /**
   * 紧随连续
   * next 紧随之后
   * 2> {first=[WaterSensor(sensor_1,1607527996000,22)], second=[WaterSensor(sensor_2,1607527999000,24)]}
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20)], second=[WaterSensor(sensor_2,1607527992000,22)]}
   */
  @Test
  def next(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // sensor_1 之后紧随 sensor_2
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      }).next("second").where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
      })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("strict")
  }

  /**
   * 不要紧随 notNext
   * 2> {first=[WaterSensor(sensor_1,1607527994000,21)]}
   */
  @Test
  def notNext(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // sensor_1 之后不紧随 sensor_2
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).notNext("second").where(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
    })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("strict")
  }

  /**
   * 松散连续 followBy
   * 3> {first=[WaterSensor(sensor_1,1607527996000,22)], second=[WaterSensor(sensor_2,1607527999000,24)]}
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20)], second=[WaterSensor(sensor_2,1607527992000,22)]}
   * 2> {first=[WaterSensor(sensor_1,1607527994000,21)], second=[WaterSensor(sensor_2,1607527999000,24)]}
   */
  @Test
  def followBy(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // sensor_1 之后出现 sensor_2，但不一定是紧随
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).followedBy("second").where(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
    })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("followBy")
  }

  /**
   * notFollowedBy 两个条件之间不要夹杂指定条件，注意notFollowedBy 不能放在末尾
   * 1> {first=[WaterSensor(sensor_1,1607527994000,21)], third=[WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def notFollowBy(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 两个sensor_1之间不出现sensor_2
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).notFollowedBy("second").where(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
    }).followedBy("third").where(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("notFollowBy")
  }

  /**
   * followedByAny 更松散的匹配，前面条件与后面所有出现条件都匹配一次
   * a、b、c、c {a,c} 条件匹配
   * a followBy c => (a,c)
   * a followByAny c => (a,c)、(a,c)
   *
   * 5> {first=[WaterSensor(sensor_1,1607527992000,20)], third=[WaterSensor(sensor_2,1607527992000,22)]}
   * 8> {first=[WaterSensor(sensor_1,1607527996000,22)], third=[WaterSensor(sensor_2,1607527999000,24)]}
   * 7> {first=[WaterSensor(sensor_1,1607527994000,21)], third=[WaterSensor(sensor_2,1607527999000,24)]}
   * 6> {first=[WaterSensor(sensor_1,1607527992000,20)], third=[WaterSensor(sensor_2,1607527999000,24)]}
   */
  @Test
  def followByAny(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 第一个是sensor_1，之后出现任意个 sensor_2
    val pattern = Pattern.begin("first").where(new SimpleCondition[WaterSensor](){
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
    }).followedByAny("third").where(new SimpleCondition[WaterSensor] {
      override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
    })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("notFollowBy")
  }

  /**
   * 5> {start=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)], second=[WaterSensor(sensor_2,1607527999000,24)]}
   * 模式组：整个pattern 被 begin 或 next 元素包裹，可以理解一个子pattern就是一个组
   * 注：贪婪模式不能适用于模式组
   */
  @Test
  def patternGroup(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 出现两个sensor_1 然后出现sensor_2
    // begin 中包裹的是 模式组
    val pattern = Pattern.begin(Pattern.begin("start").where(
      new SimpleCondition[WaterSensor](){
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      }).times(2))
      .next("second")
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
      })

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("patternGroup")
  }

  /**
   * .within(Time.seconds(2)) 前面定义两个条件前后超时约束
   * 通过 PatternTimeoutFunction 筛选出不符合条件，PatternSelectFunction筛选出符合条件的
   * 1> Right({start=[WaterSensor(sensor_1,1607527992000,20)], second=[WaterSensor(sensor_2,1607527993000,22)]})
   * 6> Left({start=[WaterSensor(sensor_1,1607527996000,22)]})
   * 5> Left({start=[WaterSensor(sensor_1,1607527994000,21)]})
   */
  @Test
  def timeout(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_2", 1607527993000L, 22),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(
          new SerializableTimestampAssigner[WaterSensor] {
            override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
          }
        ))

    // 出现两个sensor_1 然后出现sensor_2
    // begin 中包裹的是 模式组
    val pattern = Pattern.begin("start").where(
      new SimpleCondition[WaterSensor](){
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      }).next("second").where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_2")
      }).within(Time.seconds(2))

    val cepDS = CEP.pattern(ds,pattern)
    cepDS.select(new PatternTimeoutFunction[WaterSensor,String]() {
      override def timeout(map: util.Map[String, util.List[WaterSensor]], l: Long): String = map.toString
    },new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timeout")
  }

  /**
   * 匹配后跳过策略，默认noSkip，即一个元素作为某个节点的一环可以参与多次匹配，允许作为某环节相同起始，多次出现在匹配项中。
   * 如下面的：WaterSensor(sensor_1,1607527992000,20) 在first节点作为起始元素，参与两次匹配
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 3> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def noSkip(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first",AfterMatchSkipStrategy.noSkip())
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.timesOrMore(2)) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

  /**
   * noSkip
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 3> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   *
   * skipToNext 起点相同时，只保留第一个，之后的跳过，例如：WaterSensor(sensor_1,1607527992000,20)作为起点只出现在较短匹配项中，较长的丢弃了
   * 8> {first=[WaterSensor(sensor_1,1607527994000,21), WaterSensor(sensor_1,1607527996000,22)]}
   * 7> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527994000,21)]}
   */
  @Test
  def skipToNext(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first",AfterMatchSkipStrategy.skipToNext())
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.timesOrMore(2)) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

  /**
   * 匹配上后整体平移无交叉
   * 6> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,21)]}
   * 7> {first=[WaterSensor(sensor_1,1607527994000,22), WaterSensor(sensor_1,1607527996000,22)]}
   */
  @Test
  def skipPastLastEvent(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527993000L, 21),
      WaterSensor("sensor_1", 1607527994000L, 22),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first",AfterMatchSkipStrategy.skipPastLastEvent())
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.timesOrMore(2)) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

  /**
   * skipToFirst 锚定头
   * 1> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,21), WaterSensor(sensor_1,1607527994000,22)]}
   * 2> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,21), WaterSensor(sensor_1,1607527994000,22), WaterSensor(sensor_1,1607527996000,22)]}
   * 8> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,21)]}
   */
  @Test
  def skipToFirst(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527993000L, 21),
      WaterSensor("sensor_1", 1607527994000L, 22),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first",AfterMatchSkipStrategy.skipToFirst("first"))
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.timesOrMore(2)) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

  /**
   * skipToLast 每锁定一次尾，就向前滑动一次，节点只能在尾中出现一次
   * 3> {first=[WaterSensor(sensor_1,1607527992000,20), WaterSensor(sensor_1,1607527993000,21)]}
   * 4> {first=[WaterSensor(sensor_1,1607527993000,21), WaterSensor(sensor_1,1607527994000,22)]}
   * 5> {first=[WaterSensor(sensor_1,1607527994000,22), WaterSensor(sensor_1,1607527996000,23)]}
   */
  @Test
  def skipToLast(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527993000L, 21),
      WaterSensor("sensor_1", 1607527994000L, 22),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 23),
      WaterSensor("sensor_2", 1607527999000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor] {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }
      ))

    val pattern = Pattern.begin("first",AfterMatchSkipStrategy.skipToLast("first"))
      .where(new SimpleCondition[WaterSensor] {
        override def filter(t: WaterSensor): Boolean = t.id.equals("sensor_1")
      })
    val cepDS = CEP.pattern(ds,pattern.timesOrMore(2)) // 一次及以上
    cepDS.select(new PatternSelectFunction[WaterSensor,String] (){
      override def select(map: util.Map[String, util.List[WaterSensor]]): String = map.toString
    }).print()

    env.execute("timesOrMore")
  }

}
