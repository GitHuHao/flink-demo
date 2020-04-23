package com.bigdata.flink.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object RemoteEnvironment {

  /**
   * 1.启动flink 集群，无论standalone 或 cluster on yarn 都可以
   * 2.注册 createRemoteEnvironment 中填写远程job mamager 信息，jarFiles 信息一般在打包完毕后才能看到
   * 3.打jar包,flink-streaming-scala_2.11 不能为 privided
   * 4.提交运行flink run examples/streaming/streaming-demo.jar --port 9000
   * 5.运行过程去 job manager wei ui 查看进度 http://hadoop01:8081
   * 6.Stdout 或 日志输出去 TaskManager 点击正在运行任务，分别取 Stdout 或 Logs 查看
   * 7.能查看日志前提conf/flink-conf.yaml 中 设置了historyserver.archive.fs.dir 收集运行完任务的信息，jobmanager.archive.fs.dir 收集提交任务的信息
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // the port to connect to
    val port: Int = try {
      ParameterTool.fromArgs(args).getInt("port")
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
        return
      }
    }
    val env = StreamExecutionEnvironment.createRemoteEnvironment("hadoop01",9000,"/Users/huhao/softwares/idea_proj/flink-demo/streaming-demo/target/streaming-demo-1.0-SNAPSHOT.jar")
    val stream = env.readTextFile("hdfs://hadoop01:9000/apps/mr/wc/in/1.txt")
    stream.print().setParallelism(1) // 合并并行度 在最后设置
    env.execute("connect to remote env")
  }

}
