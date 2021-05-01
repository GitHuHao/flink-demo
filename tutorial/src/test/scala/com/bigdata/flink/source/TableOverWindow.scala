package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.{$, UNBOUNDED_RANGE, UNBOUNDED_ROW, lit, rowInterval}
import org.apache.flink.table.api.{DataTypes, Over, Session, Slide, Table, TableConfig, TableResult, Tumble}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, table2RowDataStream}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.junit.Test

import java.time.Duration

class TableOverWindow {

  /**
   * API 风格 Range开窗统计
   * Over 窗口：可以理解为hive 开窗函数，累计求和
   * Over.partitionBy(开窗分区字段).orderBy($(开窗排序字段)).preceding(UNBOUNDED_RANGE 按时间划分窗口 或 UNBOUNDED_ROW 按行数划分窗口 ).as(别名)
   * sum().over($("w") 累计求和
   *
   * UNBOUNDED_RANGE ：同时到达元素属于同一个窗口
   * UNBOUNDED_ROW 同时到达元素属于不同窗口
   *
   * +----+--------------------------------+-------------------------+-------------+
   * | op |                             id |                      ts |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------+
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |           2 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           2 | << 同时间到达，按一个窗口处理
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           2 | <<
   * | +I |                       sensor_2 |     2020-12-09T15:33:14 |           5 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:16 |          13 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:20 |          24 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |          34 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |          34 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:16 |          58 |
   * +----+--------------------------------+-------------------------+-------------+
   *
   */
  @Test
  def overWindowByRange1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_2", 1607527990000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527996000L, 11),
      WaterSensor("sensor_2", 1607527995000L, 5),
      WaterSensor("sensor_2", 1607527995000L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
      WaterSensor("sensor_1", 1607528000000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq, false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    val table = tabEnv.fromDataStream(ds, $("id"), $("ts").rowtime(), $("vc")) // 声明事件时间
      .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_RANGE).as("w")) // 定义滚动窗口，并取别名 lit： 过去的
      .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")).as("vc_sum"))
      .execute()

    table.print()

    env.execute("overWindowByRange1")
  }

  /**
   * SQL 风格 Range 开窗统计
   * sum(vc) over(partition by id order by t) as vc_sum
   */
  @Test
  def overWindowByRange2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
         |  id string,
         |  ts bigint,
         |  vc int,
         |  t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - interval '5' second
         |) with (
         | 'connector'='filesystem',
         | 'path'='${CommonSuit.getFile("sensor/sensor3.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    val table1 = tabEnv.executeSql(
      s"""select id,
         |ts,
         |vc,
         |sum(vc) over(partition by id order by t) as vc_sum
         |from sensor
         |""".stripMargin)

    table1.print()
  }

  /**
   * API 风格开窗 最近n秒窗口内记录累计
   * Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(1).second()).as("w")
   * +----+--------------------------------+-------------------------+-------------+-------------+
   * | op |                             id |                      ts |          vc |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------+-------------+
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |           2 |           2 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           1 |           2 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           1 |           2 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:14 |           3 |           3 | <<
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |           5 |          32 | << 同时间到的按两同一个窗口处理
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |          24 |          32 | <<
   * | +I |                       sensor_2 |     2020-12-09T15:33:16 |          24 |          53 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:16 |          11 |          11 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:20 |          11 |          11 |
   * +----+--------------------------------+-------------------------+-------------+-------------+
   */
  @Test
  def overWindowByBefore2SecondRange1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_2", 1607527990000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527996000L, 11),
      WaterSensor("sensor_2", 1607527995000L, 5),
      WaterSensor("sensor_2", 1607527995000L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
      WaterSensor("sensor_1", 1607528000000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq, false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    val table = tabEnv.fromDataStream(ds, $("id"), $("ts").rowtime(), $("vc")) // 声明事件时间
      .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(lit(1).second()).as("w")) // 定义滚动窗口，并取别名 lit： 过去的
      .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")).as("vc_sum"))
      .execute()

    table.print()

    env.execute("overWindowByRange1")
  }

  /**
   * SQL 风格开窗，基于时间统计 n秒前至当前行窗口累计和
   */
  @Test
  def overWindowByBefore2SecondRange2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
         |  id string,
         |  ts bigint,
         |  vc int,
         |  t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - interval '5' second
         |) with (
         | 'connector'='filesystem',
         | 'path'='${CommonSuit.getFile("sensor/sensor3.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    val table1 = tabEnv.executeSql(
      s"""select id,
         |ts,
         |vc,
         |sum(vc) over(partition by id order by t range between INTERVAL '1' second PRECEDING and current row) as vc_sum
         |from sensor
         |""".stripMargin)

    table1.print()
  }


  /**
   * +----+--------------------------------+-------------------------+-------------+
   * | op |                             id |                      ts |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------+
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |           2 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:14 |           5 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           1 | << 同时间到达仍按两个窗口处理
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           2 | <<
   * | +I |                       sensor_1 |     2020-12-09T15:33:16 |          13 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:20 |          24 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |          10 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |          34 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:16 |          58 |
   * +----+--------------------------------+-------------------------+-------------+
   */
  @Test
  def overWindowByUnboundedRows1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_2", 1607527990000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527996000L, 11),
      WaterSensor("sensor_2", 1607527995000L, 5),
      WaterSensor("sensor_2", 1607527995000L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
      WaterSensor("sensor_1", 1607528000000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq, false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    val table = tabEnv.fromDataStream(ds, $("id"), $("ts").rowtime(), $("vc")) // 声明事件时间
      .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(UNBOUNDED_ROW).as("w")) // 定义滚动窗口，并取别名 lit： 过去的
      .select($("id"), $("ts"), $("vc").sum().over($("w")).as("vc_sum"))
      .execute()

    table.print()

    env.execute("overWindowByUnboundedRows1")
  }

  /**
   * SQL风格 Row开窗统计
   * window w as (partition by id order by t rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 从头累计到现在
   */
  @Test
  def overWindowByUnboundedRows2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
         |  id string,
         |  ts bigint,
         |  vc int,
         |  t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - interval '5' second
         |) with (
         | 'connector'='filesystem',
         | 'path'='${CommonSuit.getFile("sensor/sensor3.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    // 窗口复用写法
    val table1 = tabEnv.executeSql(
      s"""select id,
         |vc,
         |sum(vc) over w as vc_sum,
         |count(vc) over w as vc_cnt
         |from sensor
         |window w as (partition by id order by t rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
         |""".stripMargin)

    // 窗口不复用写法
    //    val table2 = tabEnv.executeSql(
    //      s"""select id,
    //         |vc,
    //         |sum(vc) over(partition by id order by t rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) w as vc_sum,
    //         |count(vc) over(partition by id order by t rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) w as vc_cnt
    //         |from sensor
    //         |""".stripMargin)

    table1.print()
  }

  /**
   * API 风格 Row开窗统计
   * Over.partitionBy(分组开窗字段).orderBy(排序字段).preceding(rowInterval(当前行往前n行组成的窗口)).as("w")
   * +----+--------------------------------+-------------------------+-------------+-------------+
   * | op |                             id |                      ts |          vc |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------+-------------+
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |           2 |           2 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           1 |           1 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |           1 |           2 | 1 + 1 << 同时到达依旧按 两个窗口处理
   * | +I |                       sensor_2 |     2020-12-09T15:33:14 |           3 |           5 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:16 |          11 |          12 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:20 |          11 |          22 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |           5 |           8 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |          24 |          29 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:16 |          24 |          48 |
   * +----+--------------------------------+-------------------------+-------------+-------------+
   */
  @Test
  def overWindowByBefore1RowRows1(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_2", 1607527990000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527996000L, 11),
      WaterSensor("sensor_2", 1607527995000L, 5),
      WaterSensor("sensor_2", 1607527995000L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
      WaterSensor("sensor_1", 1607528000000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq, false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    val table = tabEnv.fromDataStream(ds, $("id"), $("ts").rowtime(), $("vc")) // 声明事件时间
      .window(Over.partitionBy($("id")).orderBy($("ts")).preceding(rowInterval(1)).as("w")) // 定义滚动窗口，并取别名 lit： 过去的
      .select($("id"), $("ts"), $("vc"), $("vc").sum().over($("w")).as("vc_sum"))
      .execute()

    table.print()

    env.execute("overWindowByRows1")
  }

  /**
   * SQL 风格开窗 最近n行累计
   */
  @Test
  def overWindowByBefore1RowRows2(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
         |  id string,
         |  ts bigint,
         |  vc int,
         |  t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - interval '5' second
         |) with (
         | 'connector'='filesystem',
         | 'path'='${CommonSuit.getFile("sensor/sensor3.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    // 窗口复用写法
    val table1 = tabEnv.executeSql(
      s"""select id,
         |vc,
         |sum(vc) over w as vc_sum,
         |count(vc) over w as vc_cnt
         |from sensor
         |window w as (partition by id order by t rows BETWEEN 1 PRECEDING AND CURRENT ROW)
         |""".stripMargin)

    // 窗口不复用写法
    //    val table2 = tabEnv.executeSql(
    //      s"""select id,
    //         |vc,
    //         |sum(vc) over(partition by id order by t rows BETWEEN 1 PRECEDING AND CURRENT ROW)) w as vc_sum,
    //         |count(vc) over(partition by id order by t rows BETWEEN 1 PRECEDING AND CURRENT ROW)) w as vc_cnt
    //         |from sensor
    //         |""".stripMargin)

    table1.print()
  }


}
