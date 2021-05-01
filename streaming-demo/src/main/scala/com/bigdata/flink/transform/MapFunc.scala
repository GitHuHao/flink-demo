package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.MapFunction

class MapFunc extends MapFunction[String,String]{
  override def map(t: String): String = t.toUpperCase
}

