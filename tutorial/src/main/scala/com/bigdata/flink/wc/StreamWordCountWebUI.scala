package com.bigdata.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.configuration.Configuration

/**
 * 流处理wordcount
 */
object StreamWordCountWebUI {

  // --host localhost --port 9000
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host","localhost")
    val port = params.getInt("port",9000)

    val conf = new Configuration()
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    val textDataStream = env.socketTextStream(host, port)

    textDataStream.flatMap(_.split("\\s+"))
      .filter(_.nonEmpty)
      .map((_,1))
      .keyBy(0)
      .sum(1)
      .print()

    env.execute()
  }

}
