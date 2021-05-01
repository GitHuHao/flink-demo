package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

import java.time.Duration

/**
 * 水位超过5cm，通过侧输出流发送报警
 */
class SideOutput {

  @Test
  def waterSensorAlert(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527993000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527997000L, 24))

    val ds = env.addSource(new StreamSourceMock[WaterSensor](seq))

    val dsWithTs = ds.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor] {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }))

    val alertTag = new OutputTag[WaterSensor]("over 5cm alert")

    val mainDS = dsWithTs.keyBy(_.id)
      .process(new KeyedProcessFunction[String,WaterSensor,WaterSensor] {

        private var value:ValueState[WaterSensor] = _

        override def open(parameters: Configuration): Unit = {
          val valueStateDesc = new ValueStateDescriptor("special-sensor",classOf[WaterSensor])
          value = getRuntimeContext.getState(valueStateDesc)
        }

        override def processElement(i: WaterSensor, context: KeyedProcessFunction[String, WaterSensor, WaterSensor]#Context,
                                    collector: Collector[WaterSensor]): Unit = {
          if(i.vc>=5){
            context.output(alertTag,i)
          }
          val sensor = value.value()
          val waterSensor:WaterSensor = if(sensor!=null){
            val ts = math.max(sensor.ts,i.ts)
            val vc = sensor.vc + i.vc
            WaterSensor(sensor.id,ts,vc)
          }else{
            i
          }
          value.update(waterSensor)
          collector.collect(waterSensor)
        }
      })

    mainDS.getSideOutput(alertTag).print("alert")
    mainDS.print("main")

    env.execute("waterSensorAlert")
  }


}
