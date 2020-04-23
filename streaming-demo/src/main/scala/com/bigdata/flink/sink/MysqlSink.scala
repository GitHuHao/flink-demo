package com.bigdata.flink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.bigdata.flink.bean.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.SinkFunction.Context
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction}

// 只有 RichSourceFunction 才能执行 addSink
class MysqlSink(props:Properties,sql:String) extends RichSinkFunction[Student] {


  var conn:Connection = null
  var ps:PreparedStatement = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    initConnection()
  }

  def initConnection(): Unit ={
    try {
      Class.forName(props.getProperty("className"))
      conn = DriverManager.getConnection(
        props.getProperty("url"),
        props.getProperty("user"),
        props.getProperty("password")
      )
      ps = conn.prepareStatement(sql)
    } catch {
      case e:Exception => e.printStackTrace()
    }
  }

  override def invoke(student: Student, context: Context[_]): Unit = {
    ps.setInt(1,student.id)
    ps.setString(2,student.name)
    ps.setString(3,student.password)
    ps.setInt(4,student.age)
    ps.executeUpdate()
  }

  override def close(): Unit = {
    super.close()
    if(ps!=null){
      ps.close()
    }
    if(conn!=null){
      conn.close()
    }
  }


}
