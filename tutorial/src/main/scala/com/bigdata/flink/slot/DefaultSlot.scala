package com.bigdata.flink.slot

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object DefaultSlot {
  def main(args: Array[String]): Unit = {
    // Run(EditConfig) > Program Arguments > -host localhost -port 7000
    // java -jar  -host localhost -port 7000 -c com.bigdata.flink.slot.DefaultSlot tutorial.jar
    val tool = ParameterTool.fromArgs(args)
    val host = tool.get("host","localhost")
    val port = tool.getInt("port",7000)

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
    env.setParallelism(2)

    val dataStream = env.socketTextStream(host, port)
      .flatMap(_.split("\\s+")).slotSharingGroup("1")
      .filter(_.nonEmpty).slotSharingGroup("2")
      .map((_,1))
      .keyBy(0)
      .sum(1)

    dataStream.print("wc").setParallelism(1)

    env.execute("DefaultSlot")

  }
}
