package com.bigdata.flink.sink

import java.io.PrintStream

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction}
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext

// 只有 RichSourceFunction 才能执行 addSink
class PrintSink[IN]() extends RichSinkFunction[IN]{

  var STDOUT:Boolean = false
  var STDERR:Boolean = true

  var STDTYPE:Boolean = false
  var stream:PrintStream = null
  var prefix:String = null

  def setStandardOut(): Unit ={
    STDOUT = true
    STDERR = false
    STDTYPE = STDOUT
  }

  def setStandardErr(): Unit ={
    STDOUT = false
    STDERR = true
    STDTYPE = STDERR
  }

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val context = getRuntimeContext().asInstanceOf[StreamingRuntimeContext]
    stream = if(STDTYPE==STDOUT) System.out else System.err

    if(context.getNumberOfParallelSubtasks()>1){
      prefix = context.getIndexOfThisSubtask() + 1 + "> "
    }else{
      prefix = null
    }
  }

  override def invoke(record: IN): Unit = {
    if(prefix !=null){
      stream.println(prefix+record.toString)
    }else{
      stream.println(record.toString)
    }
  }

  override def close(): Unit = {
    super.close()
    this.stream = null
    this.prefix = null
  }

  override def toString: String = {
    s"Print to ${if(STDTYPE==STDOUT) "System.out" else "System.err"}"
  }


}
