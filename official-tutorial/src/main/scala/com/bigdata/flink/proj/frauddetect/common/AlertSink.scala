package com.bigdata.flink.proj.frauddetect.common

import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.slf4j.{Logger, LoggerFactory}


class AlertSink extends SinkFunction[Alert]{

  private val logger: Logger = LoggerFactory.getLogger(classOf[AlertSink])

  override def invoke(value: Alert, context: SinkFunction.Context): Unit = {
    logger.info(value.toString)
  }

}
