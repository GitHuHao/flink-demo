package com.bigdata.flink.proj.taxi.app

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{printOrTest, rideSourceOrTest}
import org.apache.flink.util.Collector

import scala.concurrent.duration.DurationInt

/**
 * 从出发开始，2h内未收到停车消息，就判定为LongRide
 */
object LongRidesSolution {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)
    val ds = env.addSource(rideSourceOrTest(new TaxiRideGenerator))
      .keyBy(_.rideId)
      .process(new KeyedProcessFunction[Long, TaxiRide, TaxiRide]() {
        private lazy val startState: ValueState[TaxiRide] = getRuntimeContext.getState(
          new ValueStateDescriptor("start-ride", classOf[TaxiRide]))

        //        private lazy val timerState:ValueState[Long] = getRuntimeContext.getState(
        //          new ValueStateDescriptor("timer",classOf[Long]))

        override def processElement(i: TaxiRide, context: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
                                    collector: Collector[TaxiRide]): Unit = {
          val startRide = startState.value()
          if (startRide == null) {
            startState.update(i)
            if (i.isStart) {
              context.timerService().registerEventTimeTimer(getTimerTime(i))
            }
          } else {
            if (!i.isStart) {
              context.timerService().deleteEventTimeTimer(getTimerTime(startRide))
            }
            startState.clear()
          }
        }

        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#OnTimerContext,
                             out: Collector[TaxiRide]): Unit = {
          out.collect(startState.value())
          startState.clear()
        }

        // 通过函数换算得到timer时间，可以省略一个timer状态
        private def getTimerTime(ride: TaxiRide): Long = {
          ride.startTime.toEpochMilli + 2.hour.toMillis
        }

      })
    printOrTest(ds)
    env.execute("LongRidesSolution")
  }


}
