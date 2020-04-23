package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.FilterFunction

class FilterFunc extends FilterFunction[String]{
  override def filter(t: String): Boolean = t.contains("i")
}
