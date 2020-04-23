package com.bigdata.flink.transform

import org.apache.flink.api.java.functions.KeySelector

// 奇偶分区
class KeyFunc extends KeySelector[String,Int]{
  override def getKey(in: String): Int = {
    val i = in.charAt(0).asInstanceOf[Int]
//    println(s"${in} -> ${i}")
    i % 2
  }
}
