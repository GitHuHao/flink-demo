package com.bigdata.flink.source

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import com.bigdata.flink.bean.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}

class MysqlSource(props:Properties,sql:String) extends RichSourceFunction[Student]{

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

  override def run(sourceContext: SourceFunction.SourceContext[Student]): Unit = {
    val resultSet = ps.executeQuery()
    while(resultSet.next()){
      val student = Student(resultSet.getInt("id"),
        resultSet.getString("name").trim(),
        resultSet.getString("password").trim(),
        resultSet.getInt("age")
      )
      sourceContext.collect(student)
    }
  }

  override def cancel(): Unit = {

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
