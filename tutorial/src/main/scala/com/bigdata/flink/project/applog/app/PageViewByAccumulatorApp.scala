package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.UserBehavior
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.api.common.accumulators.{Accumulator, LongCounter}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

// 累加器思路实现pv统计
object PageViewByAccumulatorApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC) // 处理文件，适合用批模式
    val ds = env.readTextFile(CommonSuit.getFile("applog/UserBehavior.csv"))
    // 543462,1715,1464116,pv,1511658000
    ds.map{line=>
      val arr = line.split(",")
      UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
    }.process(
      new ProcessFunction[UserBehavior,Long](){
        val counter = new LongCounter()

        override def open(parameters: Configuration): Unit = {
          getRuntimeContext.addAccumulator("pv-count", counter)
        }
        override def processElement(i: UserBehavior, context: ProcessFunction[UserBehavior, Long]#Context,
                                    collector: Collector[Long]): Unit = {
          if(i.behavior.equals("pv")){
            counter.add(1)
            collector.collect(counter.getLocalValue)
          }
        }
      }
    ).print()
    env.execute("PageViewApp")
  }

}
