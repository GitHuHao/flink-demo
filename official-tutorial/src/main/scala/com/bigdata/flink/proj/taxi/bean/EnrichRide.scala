package com.bigdata.flink.proj.taxi.bean

import org.apache.flink.training.exercises.common.datatypes.TaxiRide
import org.apache.flink.training.exercises.common.utils.GeoUtils

// 默认参数不需要new
case class EnrichRide(var startCell:Int=Int.MinValue,var endCell:Int=Int.MinValue) extends TaxiRide {

  // 需要new
  def this(ride: TaxiRide){
    this()
    this.startCell = GeoUtils.mapToGridCell(ride.startLon,ride.startLat)
    this.endCell = GeoUtils.mapToGridCell(ride.endLon,ride.endLat)
  }

}
