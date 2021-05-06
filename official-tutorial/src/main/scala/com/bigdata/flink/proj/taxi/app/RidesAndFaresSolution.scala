package com.bigdata.flink.proj.taxi.app

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.training.exercises.common.datatypes.{TaxiFare, TaxiRide}
import org.apache.flink.training.exercises.common.sources.{TaxiFareGenerator, TaxiRideGenerator}
import org.apache.flink.training.exercises.common.utils.ExerciseBase
import org.apache.flink.training.exercises.common.utils.ExerciseBase.{fareSourceOrTest, printOrTest, rideSourceOrTest}
import org.apache.flink.util.Collector

object RidesAndFaresSolution {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val fareDS = env.addSource(fareSourceOrTest(new TaxiFareGenerator)).keyBy(_.rideId)
    val rideDS = env.addSource(rideSourceOrTest(new TaxiRideGenerator)).filter(_.isStart).keyBy(_.rideId)

    val ds = rideDS.connect(fareDS).flatMap(new RichCoFlatMapFunction[TaxiRide, TaxiFare, (TaxiRide, TaxiFare)]() {
      private lazy val rideState = getRuntimeContext.getState(new ValueStateDescriptor("ride-state", classOf[TaxiRide]))
      private lazy val fareState = getRuntimeContext.getState(new ValueStateDescriptor("fare-state", classOf[TaxiFare]))

      override def flatMap1(in1: TaxiRide, collector: Collector[(TaxiRide, TaxiFare)]): Unit = {
        val fare = fareState.value
        if (fare != null) {
          fareState.clear()
          collector.collect((in1, fare))
        } else {
          rideState.update(in1)
        }
      }

      override def flatMap2(in2: TaxiFare, collector: Collector[(TaxiRide, TaxiFare)]): Unit = {
        val ride = rideState.value
        if (ride != null) {
          rideState.clear()
          collector.collect((ride, in2))
        } else {
          fareState.update(in2)
        }
      }

      //      lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      //        new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
      //      lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      //        new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))
      //
      //      override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      //        val fare = fareState.value
      //        if (fare != null) {
      //          fareState.clear()
      //          out.collect((ride, fare))
      //        }
      //        else {
      //          rideState.update(ride)
      //        }
      //      }
      //
      //      override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      //        val ride = rideState.value
      //        if (ride != null) {
      //          rideState.clear()
      //          out.collect((ride, fare))
      //        }
      //        else {
      //          fareState.update(fare)
      //        }
      //      }

    })

    //    val ds = rideDS.connect(fareDS).process(new KeyedCoProcessFunction[Long,TaxiRide,TaxiFare,(TaxiRide,TaxiFare)]() {
    //
    //      private lazy val rideState = getRuntimeContext.getState(new ValueStateDescriptor("ride-state",classOf[TaxiRide]))
    //      private lazy val fareState = getRuntimeContext.getState(new ValueStateDescriptor("fare-state",classOf[TaxiFare]))
    //
    //      override def processElement1(in1: TaxiRide, context: KeyedCoProcessFunction[Long, TaxiRide,
    //        TaxiFare, (TaxiRide, TaxiFare)]#Context, collector: Collector[(TaxiRide, TaxiFare)]): Unit = {
    //        val fare = fareState.value()
    //        if(fare!= null){
    //          collector.collect((in1,fare))
    //          fareState.clear()
    //        }else{
    //          rideState.update(in1)
    //        }
    //      }
    //
    //      override def processElement2(in2: TaxiFare, context: KeyedCoProcessFunction[Long, TaxiRide,
    //        TaxiFare, (TaxiRide, TaxiFare)]#Context, collector: Collector[(TaxiRide, TaxiFare)]): Unit = {
    //        val ride = rideState.value()
    //        if(ride!=null){
    //          collector.collect((ride,in2))
    //          rideState.clear()
    //        }else{
    //          fareState.update(in2)
    //        }
    //      }
    //    })
    //
    printOrTest(ds)

    env.execute("RidesAndFaresSolution")
  }


  def main2(args: Array[String]) {

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
    // keyed, managed state
    lazy val rideState: ValueState[TaxiRide] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiRide]("saved ride", classOf[TaxiRide]))
    lazy val fareState: ValueState[TaxiFare] = getRuntimeContext.getState(
      new ValueStateDescriptor[TaxiFare]("saved fare", classOf[TaxiFare]))

    override def flatMap1(ride: TaxiRide, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val fare = fareState.value
      if (fare != null) {
        fareState.clear()
        out.collect((ride, fare))
      }
      else {
        rideState.update(ride)
      }
    }

    override def flatMap2(fare: TaxiFare, out: Collector[(TaxiRide, TaxiFare)]): Unit = {
      val ride = rideState.value
      if (ride != null) {
        rideState.clear()
        out.collect((ride, fare))
      }
      else {
        fareState.update(fare)
      }
    }
  }

}
