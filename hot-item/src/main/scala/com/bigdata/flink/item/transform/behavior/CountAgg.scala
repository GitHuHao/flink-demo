package com.bigdata.flink.item.transform.behavior

import com.bigdata.flink.item.bean.behavior.UserBehavior
import org.apache.flink.api.common.functions.AggregateFunction

// 聚合函数，按keyed对象粒度，时刻都在执行
class CountAgg extends AggregateFunction[UserBehavior,Long,Long]{
  override def createAccumulator(): Long = 0l

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc + acc1
}
