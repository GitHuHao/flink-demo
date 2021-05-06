package com.bigdata.flink.proj.taxi.app

import org.apache.flink.api.common.state.{MapState, MapStateDescriptor}
import org.apache.flink.api.common.time.{Time =>CTime}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.training.exercises.common.datatypes.TaxiFare
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.util.Collector
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{fareSourceOrTest, printOrTest}

/**
 * 不使用flink的滚动窗口前提下，借助state、timer实现窗口统计
 */
object HourlyTipsSolution2 {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(fareSourceOrTest(new TaxiFareGenerator()))

    val lateTag = new OutputTag[TaxiFare]("late-fare")

    val sumDS = ds.keyBy(_.driverId).process(new HourlySumProcessFunction(CTime.hours(1),lateTag))
      .timeWindowAll(Time.hours(1))
      .maxBy(2)

//    sumDS.print("sum")
//    sumDS.getSideOutput(lateTag).print("late")

    printOrTest(sumDS)

    env.execute("HourlyTipsSolution2")
  }

  class HourlySumProcessFunction(window:CTime,lateTag:OutputTag[TaxiFare]) extends KeyedProcessFunction[Long,TaxiFare,(Long,Long,Float)]{
    private var sumOfTips:MapState[Long,Float] = _

    override def open(parameters: Configuration): Unit = {
      sumOfTips = getRuntimeContext.getMapState(new MapStateDescriptor("timer-sum",classOf[Long],classOf[Float]))
    }

    override def processElement(fare: TaxiFare, context: KeyedProcessFunction[Long, TaxiFare, (Long, Long, Float)]#Context,
                                collector: Collector[(Long, Long, Float)]): Unit = {
      val eventTime = fare.getEventTime
      val watermark = context.timerService().currentWatermark()
      if(eventTime <= watermark){
        // 迟到数据
        context.output(lateTag,fare)
      }else{
        val triggerAt = getWindowEnd(eventTime)
        context.timerService().registerEventTimeTimer(triggerAt)
        var windowSum = sumOfTips.get(triggerAt)
        if(windowSum == null){
          windowSum = 0.0F
        }
        windowSum += fare.tip
        sumOfTips.put(triggerAt,windowSum)
      }
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TaxiFare, (Long, Long, Float)]#OnTimerContext,
                         out: Collector[(Long, Long, Float)]): Unit = {
      val windowSum = sumOfTips.get(timestamp)
      out.collect((timestamp +1,ctx.getCurrentKey,windowSum))
      sumOfTips.remove(timestamp)
    }

    private def getWindowEnd(tmstp:Long): Long ={
      // 提前1ms 触发提交
      tmstp - (tmstp % window.toMilliseconds) + window.toMilliseconds - 1
    }

  }

}
