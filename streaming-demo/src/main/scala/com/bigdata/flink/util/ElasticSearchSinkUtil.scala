package com.bigdata.flink.util

import java.util.{ArrayList => AList}
import java.net.{MalformedURLException, URL}
import collection.mutable.{Set => MSet}
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost

import scala.collection.JavaConversions._

object ElasticSearchSinkUtil {

  def addSink[T](hosts:Set[HttpHost], bulkFlushMaxActions:Int, parallelism:Int,
                 operator:DataStream[T], func:ElasticsearchSinkFunction[T],
                 handler:ActionRequestFailureHandler): Unit ={
    val wrapper = new AList[HttpHost]()
    wrapper.addAll(hosts)
    val builder = new ElasticsearchSink.Builder[T](wrapper, func)
    builder.setBulkFlushMaxActions(bulkFlushMaxActions)
    builder.setFailureHandler(handler)
    operator.addSink(builder.build()).setParallelism(parallelism)
  }

  def extractHost(hosts:String): Set[HttpHost] ={
    val strings = hosts.split(",").toList
    val addresses = MSet[HttpHost]()
    for(host <- strings){
      if(host.startsWith("http")){
        val url = new URL(host)
        addresses.add(new HttpHost(url.getHost,url.getPort))
      }else{
        val parts = host.split(":", 2)
        if(parts.length>1){
          addresses.add(new HttpHost(parts(0),Integer.parseInt(parts(1))))
        }else{
          throw new MalformedURLException("invalid elasticsearch hosts format")
        }
      }
    }
    addresses.toSet
  }


}
