package com.bigdata.flink.proj.taxi.app

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{fareSourceOrTest, printOrTest}
import org.apache.flink.util.Collector

/**
 * 统计最近一小时收入最高司机
 *
 * 注：
 * 1.直接使用了TumblingEventTimeWindows，但没有找到设置时间语义和水印相关代码，因为这些在TaxiFareGenerator中都已经设置到ctx里面；
 * 2.程序执行流程
 * 创建数据源(水印、时间) -> map 字段裁剪 -> keyBy 分组 -> window 开窗 -> reduce(窗口聚合,打上窗口时间标记) -> timeWindowAll 全窗口收集 -> maxBy 窗口最高
 * 3.
 */
object HourlyTipsSolution {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)
    val ds = env.addSource(fareSourceOrTest(new TaxiFareGenerator))
      .map(f => (f.driverId, f.tip))
      .keyBy(_._1)
      .window(TumblingEventTimeWindows.of(Time.hours(1)))
      .reduce(
        (f1, f2) => (f1._1, f1._2 + f2._2),
        new ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow]() {
          override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
            out.collect((context.window.getEnd, key, elements.iterator.next()._2))
          }
        }).timeWindowAll(Time.hours(1))
      .maxBy(2) // 前面是tuple 因此这里是按位置取数

    printOrTest(ds)

    env.execute("HourTips")
  }

  class WrapWithWindowInfo() extends ProcessWindowFunction[(Long, Float), (Long, Long, Float), Long, TimeWindow] {
    override def process(key: Long, context: Context, elements: Iterable[(Long, Float)], out: Collector[(Long, Long, Float)]): Unit = {
      val sumOfTips = elements.iterator.next()._2
      out.collect((context.window.getEnd, key, sumOfTips))
    }
  }

}
