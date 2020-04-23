package com.bigdata.flink.suit

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.{Logger, LoggerFactory}

import scala.sys.process._

object CommonSuit {

  def getLogger(any:Any): Logger ={
    LoggerFactory.getLogger(any.getClass)
  }

  def parseArgs(args: Array[String]):Map[String,String]={
    val status = null!=args && args.length>0
    val arg = if(status) args.mkString("") else ""
    val params = arg.split(",").map{x=>
      val kvs = x.stripMargin.split(":")
      if(kvs.length==2){
        (kvs(0).stripMargin->kvs(1).stripMargin)
      }else{
        ("" -> "")
      }
    }.toMap
    params
  }

  def deleteLocalDir(path:String): Boolean ={
    val code = s"rm -rf ${path}".!
    code == 0
  }

  def deleteHdfsDir(path:String, url:String ="hdfs://hadoop01:9000", user:String="admin"): Boolean ={
    val dfsConf = new Configuration()
    val fs = FileSystem.get(new URI(url), dfsConf, user)
    fs.delete(new Path(path), true)
  }

  def getFile(file:String): String ={
    val path = this.getClass.getClassLoader.getResource(file).getPath
    s"chmod +x ${path}".!
    path
  }


}
