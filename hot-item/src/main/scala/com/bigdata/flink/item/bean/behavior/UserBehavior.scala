package com.bigdata.flink.item.bean.behavior

// 需要执行keyBy 操作是，必须包含空参构造器，且属性必须是var
case class UserBehavior(var userId:Long,var itemId:Long,var categoryId:Int,var behavior: String,var timestamp:Long){
  def this() {this(0l,0l,0,"",0l) }
}
