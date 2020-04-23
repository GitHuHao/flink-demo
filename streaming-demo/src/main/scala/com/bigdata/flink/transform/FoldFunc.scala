package com.bigdata.flink.transform

import org.apache.flink.api.common.functions.FoldFunction

class FoldFunc extends FoldFunction[String,String]{
  override def fold(t: String, o: String): String = t+o
}
