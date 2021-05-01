package com.bigdata.flink.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 * 流处理wordcount
 */
object StreamWordCount {

  // --host localhost --port 9000
  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host")
    val port = params.getInt("port")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
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
