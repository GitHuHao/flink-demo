package com.bigdata.flink.project.applog.app

import com.bigdata.flink.project.applog.model.LoginEvent
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Date
import scala.collection.convert.ImplicitConversions.`iterable AsScalaIterable`
import scala.jdk.CollectionConverters.seqAsJavaListConverter

/**
 * 恶意登录监控
 * 连续2次登录失败，且时间间隔小于2秒，定义为恶意登录
 */
object LoginFailAlertApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dangerLoginTag = new OutputTag[String]("danger-login")
    val mainDS = env.readTextFile(CommonSuit.getFile("applog/LoginLog.csv"))
      .map{line =>
        val arr = line.split(",")
        LoginEvent(arr(0).toLong,arr(1),arr(2),arr(3).toLong*1000L)
      }.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)).withTimestampAssigner(
      new SerializableTimestampAssigner[LoginEvent] {
        override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.eventTime
      }
    )).keyBy(_.userId)
      .process(new KeyedProcessFunction[Long,LoginEvent,LoginEvent] {
        private var failedLogin:ListState[LoginEvent] = _

        override def open(parameters: Configuration): Unit = {
          failedLogin = getRuntimeContext.getListState(new ListStateDescriptor("failedLogin",classOf[LoginEvent]))
        }

        override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context,
                                    collector: Collector[LoginEvent]): Unit = {
          // TODO matchs 是正则匹配，match 是 switch-case
          i.eventType  match {
            case "fail" => {
              failedLogin.add(i)
              val fails = failedLogin.get().toList.sortWith((l1,l2)=>l1.eventTime>l2.eventTime)
              if(fails.size==2){
                val current = fails(0)
                val last = fails(1)
                val duration = current.eventTime - last.eventTime
                if(duration <= 2000){
                  val strBuilder = new StringBuilder
                  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                  strBuilder.append(s"userId: ${current.userId} login fail twice in ${duration} ms\n")
                  strBuilder.append(s"${last.ip} ${sdf.format(new Date(last.eventTime))}\n")
                  strBuilder.append(s"${current.ip} ${sdf.format(new Date(current.eventTime))}\n")
                  context.output(dangerLoginTag,strBuilder.toString())
                }
                failedLogin.update(fails.take(0).asJava)
              }
            }
            case "success" => {
              failedLogin.clear() // 出现成功，需要清除状态
            }
          }

          collector.collect(i)
        }
      })

    mainDS.getSideOutput(dangerLoginTag).print("danger-login")

    env.execute(getClass.getSimpleName)
  }

}
