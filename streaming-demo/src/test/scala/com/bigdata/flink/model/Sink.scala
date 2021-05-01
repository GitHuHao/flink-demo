package com.bigdata.flink.model

import java.util.Properties

import com.bigdata.flink.bean.{Student}
import com.bigdata.flink.sink.{ElasticSink, FailureHandler, MysqlSink, PrintSink}
import com.bigdata.flink.source.MysqlSource
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.io.TextOutputFormat
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.core.fs.Path
import com.bigdata.flink.util.ElasticSearchSinkUtil
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.junit.Test

class Sink {

  /**
   * 直接输出到标准输出流
   */
  @Test
  def print(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[Int] = env.fromElements(1,2,3)
    stream.print("print")
    env.execute("print sink")
  }

  /**
   * 直接输出到错误输出流
   */
  @Test
  def printToErr(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[Long] = env.generateSequence(1, 4)
    stream.printToErr("print to err")
    env.execute("printToErr")
  }

  @Test
  def writeAsText(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[Long] = env.generateSequence(1, 4)
    val out = "out/text"
    CommonSuit.deleteLocalDir(out)
    stream.writeAsText(out)
    env.execute("writeAsText")
  }

  /**
   * writeAsCsv 输出对象不能包含 Null ，否则报错
   */
  @Test
  def writeAsCsv(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements((1, 'a',"java",true), (2,'b', "python",false), (3,'c',"scala",true))
    val out = "out/csv"
    CommonSuit.deleteLocalDir(out)
    stream.writeAsCsv(out)
    env.execute("writeAsCsv")
  }

  @Test
  def writeUsingOutputFormat(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements("java","scala","python")
    val out = "out/format"
    val outFormat = new TextOutputFormat[String](new Path(out))
    outFormat.setWriteMode(WriteMode.OVERWRITE)
    stream.writeUsingOutputFormat(outFormat)
    env.execute("writeUsingOutputFormat")
  }

  /**
   * 写出到 socket
   * 1.先启动监听端 nc -l 9000
   * 2.渠道发送端
   */
  @Test
  def writeToSocket(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream:DataStream[String] = env.fromElements("java\n","scala\n","python\n")
    val schema = new SimpleStringSchema()
    stream.writeToSocket("localhost", 9000, schema)
    env.execute("writeToSocket")
  }

  @Test
  def printSink(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("url","jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8")
    props.put("className","com.mysql.jdbc.Driver")
    props.put("user","test")
    props.put("password","test")

    val sql = "select * from student where age>17"
    val sqlStream = env.addSource(new MysqlSource(props, sql))

    val pSink = new PrintSink[Student]()
    pSink.setStandardErr()
    sqlStream.addSink(pSink)
    env.execute("mysql data source")
  }

  @Test
  def mysqlSink(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("url","jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8")
    props.put("className","com.mysql.cj.jdbc.Driver")
    props.put("user","test")
    props.put("password","test")

    val sourceSql = "select id,name,password,age+10 as age from student where age>17"
    val sqlStream = env.addSource(new MysqlSource(props, sourceSql))

    val sinkSql = "insert into student(id,name,password,age) values(?,?,?,?) on duplicate key update name=values(name),password=values(password),age=values(age)"
    sqlStream.addSink(new MysqlSink(props, sinkSql))

    env.execute("sink to mysql")
  }

  @Test
  def esSink(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("url","jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8")
    props.put("className","com.mysql.cj.jdbc.Driver")
    props.put("user","test")
    props.put("password","test")

    val sql = "select * from student where age>17"
    val sqlStream = env.addSource(new MysqlSource(props, sql))

    val indexName = "student"
    val sinkFunc = new ElasticSink(indexName)

    val hosts = ElasticSearchSinkUtil.extractHost("hadoop01:9200,hadoop02:9200,hadoop03:9200")
    val bulkSize = 5 // 每次提交处理批次
    val sinkParallelism = 1 // sink 并行度
    val handler = new FailureHandler()

    ElasticSearchSinkUtil.addSink(
      hosts,
      bulkSize,
      sinkParallelism,
      sqlStream,
      sinkFunc,
      handler
    )

    env.execute("mysql data source")
  }

}
