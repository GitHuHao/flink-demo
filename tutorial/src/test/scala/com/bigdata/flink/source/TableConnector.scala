package com.bigdata.flink.source

class TableConnector {

  /**
   * flink-sql直接读kafka数据
   *  1) 下载 flink-sql-connector-kafka_2.11-1.12.0.jar ，移动到flink的lib目录
   *  https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.0/flink-sql-connector-kafka_2.11-1.12.0.jar
   *
   *  2）flink启动yarn-session 部署
   *  bin/yarn-session.sh start
   *
   *  3) 启动内嵌交互查询sql
   *  bin/sql-client.sh embedded
   *
   *  4）sql客户端创建动态表映射kafka
   *  create table sensor(id string, ts bigint, vc int)
      with(
          'connector'='kafka',
          'topic'='sensor_source',
          'properties.bootstrap.servers'='hadoop01:9092,hadoop02:9092,hadoop03:9092',
          'properties.group.id'='flink-sql',
          'format'='json',
          'scan.startup.mode'='latest-offset'
      );
   *
   *  5）sql客户端启动查询
   *  select * from sensor;
   *
   *  6) 往kafka的topic sensor_source 发送数据，观察上面查询sql的变化
   *  {"id": "sensor1", "ts": 1000, "vc": 10}
   *
   *  {"id": "sensor1", "ts": 1000, "vc": 10}
   */
  def kafkaConnector(): Unit ={

  }

  /**
   * flink-sql连接Mysql
   *  1）下载mysql连接器 flink-connector-jdbc_2.11-1.12.0.jar 移动到flink的lib目录；
   *   maven 下载
   *
   *  2）flink启动yarn-session 部署
   *  bin/yarn-session.sh start
   *
   *  3) 启动内嵌交互查询sql
   *  bin/sql-client.sh embedded
   *
   *  4）sql客户端创建动态表映射mysql中已经存在的表，注意时间戳类型需要对应，不能使用bigint类型接收timestamp类型
   *  create table sensor1 (id string, ts timestamp, vc int)
      with(
          'connector'='jdbc',
          'url'='jdbc:mysql://hadoop01:3306/test',
          'username'='root',
          'password'='root',
          'table-name'='sensor'
      );
   *
   *  5）sql客户端启动查询（立即发起查询，并结束）
   *  select * from sensor1;
   *
   *  6) 往mysql的sensor中插入新数据，重新发起sql查询
   *
   *  注：与kafka流式数据不同，mysql是批处理查询，每次数据变更需要手动发起查询
   */
  def mysqlConnector(): Unit ={

  }


}
