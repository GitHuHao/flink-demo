package com.bigdata.flink.sink

import com.bigdata.flink.bean.Student
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.util.ExceptionUtils
import org.elasticsearch.ElasticsearchParseException
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException
import org.elasticsearch.common.xcontent.XContentType
import org.json4s.{DefaultFormats, JValue}
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.JsonMethods._

class ElasticSink(indexName:String) extends ElasticsearchSinkFunction[Student]{

  def createIndexRequest(student: Student):IndexRequest= {
    implicit val formats: DefaultFormats = DefaultFormats
    val jsonString =write(student)
    Requests.indexRequest()
      .index(indexName)
      .`type`("data")
      .id(student.id.toString)
      .source(jsonString, XContentType.JSON)
  }

  override def process(student: Student, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
    requestIndexer.add(createIndexRequest(student));
  }
}

class FailureHandler extends ActionRequestFailureHandler{
  override def onFailure(actionRequest: ActionRequest, throwable: Throwable, i: Int, requestIndexer: RequestIndexer): Unit = {

    if(ExceptionUtils.findThrowable(throwable,classOf[EsRejectedExecutionException]).isPresent){
      requestIndexer.add(actionRequest)
    }else if(ExceptionUtils.findThrowable(throwable,classOf[ElasticsearchParseException]).isPresent){
      throw throwable
    }else{
      throw throwable
    }
  }
}
