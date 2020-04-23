package com.bigdata.flink.item.transform.login

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.login.{LoginEvent, Warning}
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import scala.collection.JavaConversions._

class FailMatchByCompare extends KeyedProcessFunction[Long,LoginEvent,Warning]{
  private lazy val failLoginState = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("saved failing login",classOf[LoginEvent]))
  private lazy val sdf = new SimpleDateFormat("yyyy/MM/dd-HH:mm:ss")
  override def processElement(event: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, collector: Collector[Warning]): Unit = {
    if(event.eventType=="fail"){
      val failIter = failLoginState.get().iterator()
      if(failIter.hasNext){
        val lastFail = failIter.next()
        if(event.eventTime-lastFail.eventTime<2000){
          val msg = s"login failed in 2 seconds"
          collector.collect(Warning(lastFail.userId,sdf.format(new Date(lastFail.eventTime)),sdf.format(new Date(event.eventTime)),msg))
        }
        failLoginState.update(List(event))
      }else{
        failLoginState.add(event)
      }
    }else{
      failLoginState.clear()
    }
  }
}
