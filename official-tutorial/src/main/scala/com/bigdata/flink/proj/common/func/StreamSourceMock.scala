package com.bigdata.flink.proj.common.func

import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util.concurrent.TimeUnit

class StreamSourceMock[T](seq:Seq[T],printable:Boolean=true) extends SourceFunction[T]{
  private var running = true

  override def run(sourceContext: SourceFunction.SourceContext[T]): Unit = {
    for(ele <- seq){
      if(printable){
        println(s"send ${ele} ")
      }
      sourceContext.collect(ele)
      TimeUnit.SECONDS.sleep(1)
    }
  }

  override def cancel(): Unit = {
    running = false
  }

}
