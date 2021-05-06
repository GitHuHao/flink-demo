package com.bigdata.flink.proj.adult

import org.apache.flink.streaming.api.scala._

case class Person(name:String,age:Int)

object AdultFilterApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(Person("a",12),Person("a",32),Person("a",22))
      .filter(_.age>18)
      .print()
    env.execute("AdultFilterApp")
  }

}
