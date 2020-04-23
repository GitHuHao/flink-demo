package com.bigdata.flink.item.bean.behavior

import java.nio.charset.StandardCharsets

import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization._

class BehaviorSchema extends DeserializationSchema[UserBehavior] with SerializationSchema[UserBehavior]{

  override def deserialize(bytes: Array[Byte]): UserBehavior = {
    implicit val formats: DefaultFormats = DefaultFormats
    read[UserBehavior](new String(bytes, StandardCharsets.UTF_8))
  }

  override def isEndOfStream(t: UserBehavior): Boolean = false

  override def serialize(behavior: UserBehavior): Array[Byte] = {
    implicit val formats: DefaultFormats = DefaultFormats // 不能被序列化，只能放在函数中，以局部变量形式存在，不能作为成员变量
    write(behavior).getBytes(StandardCharsets.UTF_8)
  }

  override def getProducedType: TypeInformation[UserBehavior] = TypeInformation.of(classOf[UserBehavior])
}
