package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.JoinFunction

class JoinFunc extends JoinFunction[(String,Int),(String,Int),(String,Int,Int)]{
  override def join(in1: (String, Int), in2: (String, Int)): (String, Int, Int) = {
    (in1._1,in1._2,in2._2)
  }
}
