package com.bigdata.flink.item.transform.ad

import com.bigdata.flink.item.bean.ad.AdClickLog
import org.apache.flink.api.common.functions.AggregateFunction

class CountAgg extends AggregateFunction[AdClickLog,Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(in: AdClickLog, acc: Long): Long = acc + 1l

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
