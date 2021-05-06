package com.bigdata.flink.proj.taxi.app

import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.sources.TaxiRideGenerator
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{printOrTest, rideSourceOrTest}
import org.apache.flink.training.exercises.common.utils.GeoUtils

/**
 * 过滤出：起点和中断都在 NewYork 的行程
 */
object RideCleansingSolution {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.addSource(rideSourceOrTest(new TaxiRideGenerator))
      .filter(r => GeoUtils.isInNYC(r.startLon, r.startLat) && GeoUtils.isInNYC(r.endLon, r.endLat))
    printOrTest(ds)
    env.execute("RideCleansingSolution")
  }

}
