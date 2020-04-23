package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.ReduceFunction

class ReduceFunc extends ReduceFunction[String]{
  override def reduce(t: String, t1: String): String = t+t1
}
