package com.bigdata.flink.proj.spendreport

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{Table, TableConfig, TableEnvironment, Tumble}
import org.apache.flink.types.Row


/**
 * 模拟器生成数据发往kafka
 * flink消费kafka消息，生成动态表，在动态表基础上上基于滚动窗口(1h)统计account_id维度总花费sum(spend) 结果写到 mysql
 * grafana 读取mysql数据进行可视化展示
 *
 * 窗口不能正常触发计算可能yuany: 数据、逻辑、窗口无法关闭。
 *
 * 注：写kafka时，如果不指定分区，就按key分配分区，很可能导致数据倾斜，设置部分分区可能根本就没有数据进入，导致窗口计算无法关闭，因此一般推荐指定分区 hash(key) % 分区数。
 *
 * 'scan.startup.mode'='latest-offset' 每次从最新开始消费
 * 'scan.startup.mode'='earliest-offset' 从最早可消费处开始消费
 *
 *
 */
object SpendReportJob {

  // $ 表达式
  def reportByApi1(rows:Table): Unit = {
    rows.window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("w"))
      .groupBy($("account_id"), $("w"))
      .select($("account_id"), $("w").start.as("log_ts"), $("amount").sum.as("amount"))
      .executeInsert("spend_report")
  }

  // 字符串
  def reportByApi2(rows:Table): Unit = {
    rows.window(Tumble.over(lit(1).hour()).on($("transaction_time")).as("w"))
      .groupBy("account_id,w")
      .select("account_id,w.start() as log_ts,sum(amount) as amount")
      .executeInsert("spend_report")
  }

  // sql
  def reportBySql(tabEnv: TableEnvironment): Unit = {
    tabEnv.executeSql(
      s"""|INSERT INTO spend_report SELECT
          | account_id,
          | TUMBLE_START(transaction_time,INTERVAL '1' HOUR) as log_ts,
          | SUM(amount) AS amount
          | FROM transactions
          | GROUP BY account_id,TUMBLE(transaction_time,INTERVAL '1' HOUR)
          |""".stripMargin)
  }

  def main(args: Array[String]): Unit = {
    // 注册table env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setParallelism(1)
    val tabEnv = StreamTableEnvironment.create(env)

    // 定义source:读取kafka数据映射为动态表，并声明水印（允许5秒延迟）
    tabEnv.executeSql(
      s"""CREATE TABLE transactions (
          | account_id BIGINT,
          | amount BIGINT,
          | transaction_time TIMESTAMP(3),
          | WATERMARK FOR transaction_time AS transaction_time - INTERVAL '10' SECOND
          |) WITH (
          | 'connector'='kafka',
          | 'topic'='tx',
          | 'properties.bootstrap.servers'='hadoop01:9092,hadoop02:9092,hadoop03:9092',
          | 'format'='csv',
          | 'scan.startup.mode'='earliest-offset'
          |)
         |""".stripMargin)

    // 定义sink:数据写到mysql，主键为account_id 和log_ts（window_start）由用户自己保重主键唯一性 NOT ENFORCED
    tabEnv.executeSql(
      s"""create table spend_report (
          |  account_id bigint,
          |  log_ts timestamp(3),
          |  amount bigint,
          |  primary key (account_id,log_ts) NOT ENFORCED
          |) with (
          | 'connector'='jdbc',
          | 'url'='jdbc:mysql://hadoop01:3306/test',
          | 'table-name'='spend_report',
          | 'driver'='com.mysql.cj.jdbc.Driver',
          | 'username'='test',
          | 'password'='test'
          |)
         |""".stripMargin)

    tabEnv.executeSql(
      s"""|SELECT
          | account_id,
          | TUMBLE_START(transaction_time,INTERVAL '1' HOUR) as log_ts,
          | SUM(amount) AS amount
          | FROM transactions
          | GROUP BY account_id,TUMBLE(transaction_time,INTERVAL '1' HOUR)
          |""".stripMargin).print()

//    val table = tabEnv.from("transactions")
//    reportByApi1(table)
//    reportByApi2(table)
//
//    reportBySql(tabEnv)
  }

}
