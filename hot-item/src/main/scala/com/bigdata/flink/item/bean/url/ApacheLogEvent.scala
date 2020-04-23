package com.bigdata.flink.item.bean.url

case class ApacheLogEvent(var ip:String,var userId:String,var eventTime:Long,var method:String,var url:String){
  def this() = this("","",0l,"","")
}
