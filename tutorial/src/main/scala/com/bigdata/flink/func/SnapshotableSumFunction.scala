package com.bigdata.flink.func

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class SnapshotableSumFunction extends MapFunction[Int,Int] with CheckpointedFunction{
  private var listState:ListState[Int] = _
  private var sum:Int = 0

  override def map(value: Int): Int = {
    listState.add(value)
    sum += value
    sum
  }

  override def snapshotState(ctx: FunctionSnapshotContext): Unit = {
    listState.clear()
    listState.add(sum)
  }

  override def initializeState(ctx: FunctionInitializationContext): Unit = {
    listState = ctx.getOperatorStateStore.getListState(new ListStateDescriptor[Int]("sum",classOf[Int]))
    listState.get().forEach(sum+=_)
  }
}
