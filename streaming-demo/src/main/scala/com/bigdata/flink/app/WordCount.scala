package com.bigdata.flink.app

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {
    val port = ParameterTool.fromArgs(args).getInt("port", 4000)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("localhost", port, '\n', 3)

    val wordCounts = stream.flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map(WordWithCount(_,1))
      .keyBy("word")
      .timeWindow(Time.seconds(4),Time.seconds(2))
      .sum("count")

    wordCounts.print().setParallelism(1)
    env.execute("Socket Window WordCount")
  }

  case class WordWithCount(word:String,count:Long)

}
