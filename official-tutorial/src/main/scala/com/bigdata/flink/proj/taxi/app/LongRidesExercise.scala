package com.bigdata.flink.proj.taxi.app

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{printOrTest, rideSourceOrTest}
import org.apache.flink.training.exercises.common.utils.{ExerciseBase, MissingSolutionException}
import org.apache.flink.util.Collector

/**
 * The "Long Ride Alerts" exercise of the Flink training in the docs.
 *
 * The goal for this exercise is to emit START events for taxi rides that have not been matched
 * by an END event during the first 2 hours of the ride.
 *
 */
object LongRidesExercise {

  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))

    val longRides = rides
      .keyBy(_.rideId)
      .process(new ImplementMeFunction())

    printOrTest(longRides)

    env.execute("Long Taxi Rides")
  }

  class ImplementMeFunction extends KeyedProcessFunction[Long, TaxiRide, TaxiRide] {

    override def open(parameters: Configuration): Unit = {
      throw new MissingSolutionException()
    }

    override def processElement(ride: TaxiRide,
                                context: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#Context,
                                out: Collector[TaxiRide]): Unit = {
    }

    override def onTimer(timestamp: Long,
                         ctx: KeyedProcessFunction[Long, TaxiRide, TaxiRide]#OnTimerContext,
                         out: Collector[TaxiRide]): Unit = {
    }

  }

}
