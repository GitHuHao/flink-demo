package com.bigdata.flink.proj.taxi.app

import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.sources.TaxiFareGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase.fareSourceOrTest
import org.apache.flink.training.exercises.common.utils.{ExerciseBase, MissingSolutionException}

/**
 * The "Hourly Tips" exercise of the Flink training in the docs.
 *
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 *
 */
object HourlyTipsExercise {

  def main(args: Array[String]) {

    // set up streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(ExerciseBase.parallelism)

    // start the data generator
    val fares = env.addSource(fareSourceOrTest(new TaxiFareGenerator()))

    throw new MissingSolutionException()

    // print result on stdout
    //    printOrTest(hourlyMax)

    // execute the transformation pipeline
    env.execute("Hourly Tips (scala)")
  }

}
