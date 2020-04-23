package com.bigdata.flink.bean

import com.google.gson.Gson
import org.apache.flink.api.common.typeinfo.TypeInformation
import java.nio.charset.Charset

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}

object MetricSchema {
  private val gson = new Gson
}

class MetricSchema extends DeserializationSchema[MetricEvent] with SerializationSchema[MetricEvent] {

  def deserialize(bytes: Array[Byte]): MetricEvent = MetricSchema.gson.fromJson(new String(bytes), classOf[MetricEvent])

  def serialize(metricEvent: MetricEvent): Array[Byte] = MetricSchema.gson.toJson(metricEvent).getBytes(Charset.forName("UTF-8"))

  def isEndOfStream(metricEvent: MetricEvent) = false

  def getProducedType: TypeInformation[MetricEvent] = TypeInformation.of(classOf[MetricEvent])
}