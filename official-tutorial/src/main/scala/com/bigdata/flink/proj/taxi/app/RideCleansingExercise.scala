package com.bigdata.flink.proj.taxi.app

import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{parallelism, printOrTest, rideSourceOrTest}
import org.apache.flink.training.exercises.common.utils.{ExerciseBase, MissingSolutionException}

/**
 * The "Ride Cleansing" exercise of the Flink training in the docs.
 *
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed to the
 * standard out.
 *
 */
object RideCleansingExercise extends ExerciseBase {

  def main(args: Array[String]) {

    // set up the execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    // get the taxi ride data stream
    val rides = env.addSource(rideSourceOrTest(new TaxiRideGenerator()))

    val filteredRides = rides
      // filter out rides that do not start and end in NYC
      .filter(ride => throw new MissingSolutionException)

    // print the filtered stream
    printOrTest(filteredRides)

    // run the cleansing pipeline
    env.execute("Taxi Ride Cleansing")
  }

}
