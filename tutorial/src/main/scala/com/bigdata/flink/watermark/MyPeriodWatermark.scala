package com.bigdata.flink.watermark

import org.apache.flink.api.common.eventtime.{Watermark, WatermarkGenerator, WatermarkGeneratorSupplier, WatermarkOutput, WatermarkStrategy}

import java.time.Duration

/**
 *
 * @param maxDelay 最大延迟
 */
class MyPeriodWatermark(var maxDelay:Duration) extends WatermarkStrategy[(String,Long)]{
  override def createWatermarkGenerator(context: WatermarkGeneratorSupplier.Context): WatermarkGenerator[(String, Long)] = {
    new WatermarkGenerator[(String, Long)] {
      // 记录已经处理元素最大时间戳
      private var maxTs = Long.MinValue

      // 每进入一个元素，就获取一个最大值
      override def onEvent(event: (String, Long), eventTimestamp: Long, output: WatermarkOutput): Unit = {
        maxTs = math.max(event._2,maxTs)
        println(s"set maxTs to ${maxTs}")
      }

      // 每隔200ms周期性发送一次水印
      override def onPeriodicEmit(output: WatermarkOutput): Unit = {
        val ts = maxTs-maxDelay.toMillis
        println(s"set watermark to ${ts}")
        output.emitWatermark(new Watermark(ts))
      }
    }
  }
}