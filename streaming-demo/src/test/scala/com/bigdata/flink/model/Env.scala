package com.bigdata.flink.model

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.junit.Test

class Env {

  /**
   * 1.直接运行时，使用本地环境
   * 2.使用提交命令时，使用集群环境
   */
  @Test
  def createExecutionEnvironment(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val file = CommonSuit.getFile("1.txt")
    val stream = env.readTextFile(file)
    stream.print()
    env.execute("create env")
  }

  /**
   * 本地环境运行
   */
  @Test
  def createLocalEnvironment(): Unit ={
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    val file = CommonSuit.getFile("1.txt")
    val stream = env.readTextFile(file)
    stream.print()
    env.execute("create local env")
  }

  /**
   * 直接将集群信息配置在代码里，直接执行，就可以使用集群环境
   * 1.启动flink 集群，无论standalone 或 cluster on yarn 都可以
   * 2.注册 createRemoteEnvironment 中填写远程job mamager 信息，jarFiles 信息一般在打包完毕后才能看到
   * 3.打jar包,flink-streaming-scala_2.11 不能为 privided
   * 4.运行过程去 job manager wei ui 查看进度 http://hadoop01:8081
   * 5.Stdout 或 日志输出去 TaskManager 点击正在运行任务，分别取 Stdout 或 Logs 查看
   * 6.能查看日志前提conf/flink-conf.yaml 中 设置了historyserver.archive.fs.dir 收集运行完任务的信息，jobmanager.archive.fs.dir 收集提交任务的信息
   */
  @Test
  def createRemoteEnvironment(): Unit ={
    val env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop01",8083,"/Users/huhao/softwares/idea_proj/flink-demo/streaming-demo/target/streaming-demo-1.0-SNAPSHOT.jar")
    val file = CommonSuit.getFile("1.txt")
    val stream = env.readTextFile(file)
    stream.print()
    env.execute("create local env")
  }



}
