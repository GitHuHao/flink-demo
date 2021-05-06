package com.bigdata.flink.proj.taxi.app

import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.{TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{fareSourceOrTest, printOrTest, rideSourceOrTest}
import org.apache.flink.training.exercises.common.utils.{ExerciseBase, MissingSolutionException}
import org.apache.flink.util.Collector

/**
 * The "Stateful Enrichment" exercise of the Flink training in the docs.
 *
 * The goal for this exercise is to enrich TaxiRides with fare information.
 *
 */
object RidesAndFaresExercise {

  def main(args: Array[String]) {

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    val rides = env
      .addSource(rideSourceOrTest(new TaxiRideGenerator()))
      .filter { ride => ride.isStart }
      .keyBy { ride => ride.rideId }

    val fares = env
      .addSource(fareSourceOrTest(new TaxiFareGenerator()))
      .keyBy { fare => fare.rideId }

    val processed = rides
      .connect(fares)
      .flatMap(new EnrichmentFunction)

    printOrTest(processed)

    env.execute("Join Rides with Fares (scala RichCoFlatMap)")
  }

  class EnrichmentFunction extends RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)] {

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      throw new MissingSolutionException()
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
    }

  }

}
