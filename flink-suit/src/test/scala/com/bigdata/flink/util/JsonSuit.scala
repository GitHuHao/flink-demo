package com.bigdata.flink.util

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._
import org.junit.Test

case class Apple(color:String,number:Int)

class FlinkSuit {

  implicit val formats = DefaultFormats

  @Test
  def jsonParse(): Unit ={
    val json = "{'color':'red','number':1}".replace("'","\"")
    val apple = read[Apple](json)
    println(apple)

    val json2 = write(apple)
    println(json2)
  }


}
