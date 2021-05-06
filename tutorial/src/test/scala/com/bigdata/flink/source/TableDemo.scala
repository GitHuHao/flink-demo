package com.bigdata.flink.source

import com.bigdata.flink.func.StreamSourceMock
import com.bigdata.flink.model.WaterSensor
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Expressions.{$, lit}
import org.apache.flink.table.api.{DataTypes, Session, Slide, Table, TableConfig, TableResult, Tumble}
import org.apache.flink.table.api.bridge.scala.{StreamTableEnvironment, table2RowDataStream}
import org.apache.flink.table.descriptors.{Csv, FileSystem, Json, Kafka, Schema}
import org.apache.flink.types.Row
import org.junit.Test

import java.time.Duration

class TableDemo {

  /**
   * 使用 点函数方式操作 TableApi
   * table.where().select
   *
   * where($("vc").isGreaterOrEqual(20)) $()表达式风格  (慎用坑比较多)
   * where("vc >= 20") 字符串表达式风格：相比于表达式坑比较少
   *
   */
  @Test
  def test1(): Unit ={
    // 创建env和tabEnv
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    // 创建流
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    // 流映射成动态表
    val table = tabEnv.fromDataStream(ds)

    // 基于动态表发起查询操作，生成新的动态表
    val subTable = table.where($("id").isEqual("sensor_1")) // $()表达式风格
      .select($("id"), $("ts"), $("vc").as("water_level"))

    // 动态表映射为append-only流
    val subDS = tabEnv.toAppendStream[Row](subTable)
    subDS.print()

    // 执行聚合逻辑
    val aggTable = subTable.where("water_level>=2") // 字符串风格
      .groupBy($("id"))
      .aggregate($("water_level").sum().as("vc_sum"))
      .select($("id"),$("vc_sum"))

    // 动态表映射为可撤销流
    val aggDS = tabEnv.toRetractStream[Row](aggTable)

    // 输出流
    aggDS.print()
    env.execute("test1")
  }

  /**
   * tabEnv.sqlQuery() 使用sql方式操作 TableApi
   */
  @Test
  def test2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)
    val table = tabEnv.fromDataStream(ds)

    val subTable = tabEnv.sqlQuery(s"select id,ts,vc as water_level from ${table} where id='sensor_1'")
    val subDS = tabEnv.toAppendStream[Row](subTable)
    subDS.print()

    val aggTable = tabEnv.sqlQuery(s"select id,sum(water_level) as vc_sum from ${subTable} where water_level>=2 group by id")
    val aggDS = tabEnv.toRetractStream[Row](aggTable)
    aggDS.print()

    env.execute("test2")
  }

  /**
   * 3> sensor_1,1607527992000,1
   * 4> sensor_1,1607527992050,1
   */
  @Test
  def table(): Unit ={
    // 创建env和tabEnv
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    // 创建流
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    // 流映射成动态表
    val sensor = tabEnv.fromDataStream(ds)

    // 直接打印表需要：import org.apache.flink.table.api.bridge.scala.table2RowDataStream 支持
    val aggTab:Table = tabEnv.sqlQuery(s"select id,ts,vc from ${sensor}")
    aggTab.print()

    env.execute("test1")
  }

  /**
   * +----+--------------------------------+----------------------+
   * | op |                             id |                  cnt |
   * +----+--------------------------------+----------------------+
   * | +I |                       sensor_1 |                    1 |
   * | -U |                       sensor_1 |                    1 |
   * | +U |                       sensor_1 |                    2 |
   * | +I |                       sensor_2 |                    1 |
   * | -U |                       sensor_2 |                    1 |
   */
  @Test
  def tableResult(): Unit ={
    // 创建env和tabEnv
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    // 创建流
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    // 流映射成动态表
    val sensor = tabEnv.fromDataStream(ds)

    // tableResult可以直接打印输出，无需execute
    val aggResult:TableResult = tabEnv.executeSql(s"select id,count(id) as cnt from ${sensor} group by id")
    aggResult.print()
  }


  /**
   *  tabEnv.toAppendStream[Row](subTable)
   * 只接收插入数据 add message，不支持修改和删除，可修改的结果强制转换为append流，会报错
   * 涉及groupBy的聚合查询，如果没有窗口约束，通常都是可修改的，不能转append流
   */
  @Test
  def toAppendStream(): Unit ={
    // 创建env和tabEnv
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    // 创建流
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    // 流映射成动态表
    val table = tabEnv.fromDataStream(ds)

    // 基于动态表发起查询操作，生成新的动态表
    val subTable = tabEnv.sqlQuery(s"select id,ts,vc as water_level from ${table} where id='sensor_1'")

    // 动态表映射为append流
    val subDS = tabEnv.toAppendStream[Row](subTable)
    subDS.print()

    env.execute("toAppendStream")
  }

  /**
   * tabEnv.toAppendStream[Row](subTable)
   * 接收新增记录 add message 和 删除记录 retract message
   * insert -> add message
   * delete -> retract message
   * update -> retract message + add message
   */
  @Test
  def toRetractStream(): Unit ={
    // 创建env和tabEnv
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    // 创建流
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    // 流映射成动态表
    val table = tabEnv.fromDataStream(ds)

    // 基于动态表发起查询操作，生成新的动态表
    val subTable = tabEnv.sqlQuery(s"select id,ts,vc from ${table} where id='sensor_1'")
    val subDS = tabEnv.toAppendStream[Row](subTable)
    subDS.print()

    val aggTable = tabEnv.sqlQuery(s"select id,count(vc) as vc_sum from ${subTable} group by id")
    val aggDS = tabEnv.toRetractStream[Row](aggTable) // (boolean,row)

    // 动态表映射为可撤销流
    aggDS.print()

    env.execute("toRetractStream")
  }

  /**
   * update流：接收upsert message 和 delete message
   * 相比于retract流，执行更新操作需要两步，update直接一步到位，效率更高
   * 注：DataStream 不能直接转换为UpsertStream
   */
  def updateStream(): Unit ={

  }

  @Test
  def fileSource(): Unit ={
    // 创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    // 创建schema
    val schema = new Schema().field("id",DataTypes.STRING())
      .field("ts",DataTypes.BIGINT())
      .field("vc",DataTypes.INT())

    // 文件 + schema 创建临时表
    tabEnv.connect(new FileSystem().path(CommonSuit.getFile("sensor/sensor.txt")))
      .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
      .withSchema(schema)
      .createTemporaryTable("sensor")

    // 聚合查询
    val wcTable = tabEnv.from("sensor").groupBy("id").select($("id"),$("id").count().as("cnt"))

    // 动态表转换为可撤销流
    val ds = tabEnv.toRetractStream[Row](wcTable)
    ds.print()

    env.execute("fileSource")
  }

  /**
   * tabEnv 读csv文件数据，写入kafka
   */
  @Test
  def kafkaSink(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env,default)

    val schema = new Schema().field("id",DataTypes.STRING())
      .field("ts",DataTypes.BIGINT())
      .field("vc",DataTypes.INT())

    tabEnv.connect(new FileSystem().path(CommonSuit.getFile("sensor/sensor.txt")))
      .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
      .withSchema(schema)
      .createTemporaryTable("sensor_source")

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    val subTab = tabEnv.fromDataStream(ds).where("id=='sensor_1'").select($("id"),$("ts"),$("vc"))

    tabEnv.toAppendStream[Row](subTab).print()

    tabEnv.connect(new Kafka()
      .version("universal")
      .topic("sensor_sink")
      .property("group.id","test")
      .property("bootstrap.servers","hadoop01:9092,hadoop02:9092,hadoop03:9092")
      .sinkPartitionerRoundRobin()
    ).withFormat(new Json())
      .withSchema(schema)
      .createTemporaryTable("sensor_sink")

    subTab.executeInsert("sensor_sink")
    env.execute("kafkaSink")
  }

  /**
   * 从kafka读，然后写入kafka
   * {"id":"sensor_1","ts":1607527992000,"vc":1}
   * {"id":"sensor_1","ts":1607527992050,"vc":1}
   * {"id":"sensor_1","ts":1607527994050,"vc":11}
   */
  @Test
  def kafkaToKafka(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)
    tabEnv.executeSql(
      s"""create table sensor_source(
         |  id string,
         |  ts bigint,
         |  vc int
         |)with(
         |  'connector'='kafka',
         |  'topic'='sensor_source',
         |  'properties.bootstrap.servers'='hadoop01:9092,hadoop02:9092,hadoop03:9092',
         |  'properties.group.id'='test',
         |  'scan.startup.mode'='latest-offset',
         |  'format'='json'
         |)
         |""".stripMargin)

    val tab = tabEnv.sqlQuery("select * from sensor_source")
    tabEnv.toAppendStream[Row](tab).print()

    tabEnv.executeSql(
      s"""create table sensor_sink(
          | id string,
          | ts bigint,
          | vc int
          |) with (
          |  'connector'='kafka',
          |  'topic'='sensor_sink',
          |  'properties.bootstrap.servers'='hadoop01:9092,hadoop02:9092,hadoop03:9092',
          |  'format'='json'
          |)
         |""".stripMargin)

//    tabEnv.executeSql("insert into sensor_sink(id,cnt) select id,count(id) as cnt from sensor_source group by id")
    tabEnv.executeSql("insert into sensor_sink(id,ts,vc) select * from sensor_source")

    env.execute("kafkaToKafka")
  }

  // 查询未注册表
  @Test
  def unregistedTable(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    val table = tabEnv.fromDataStream(ds)
    val aggTab = tabEnv.sqlQuery(s"select id,count(id) as cnt from ${table} group by id")
    tabEnv.toRetractStream[Row](aggTab).print()

    env.execute("unregistedTable")
  }

  // 查询已注册表
  @Test
  def registedTable(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))

    val table = tabEnv.fromDataStream(ds)
    tabEnv.createTemporaryView("sensor",table)

    val aggTab = tabEnv.sqlQuery(s"select id,count(id) as cnt from sensor group by id")
    tabEnv.toRetractStream[Row](aggTab).print()

    env.execute("unregistedTable")
  }

  /**
   * 处理时间proctime 表达式风格定义
   * $("pt").proctime() 必须放在schema最后定义
   */
  @Test
  def proctime1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env, default)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))
    // 处理时间
    val table = tabEnv.fromDataStream(ds,$("id"), $("ts"), $("vc"), $("pt").proctime())
    table.print()

    env.execute("proctime1")
  }

  /**
   * 处理时间2 sql风格定义：proctime() as pt
   */
  @Test
  def proctime2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env, default)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))
    // 处理时间
    val sensor = tabEnv.fromDataStream(ds)
    val subTab = tabEnv.sqlQuery(s"select id,ts,vc,proctime() as pt from ${sensor}")
    subTab.print()

    env.execute("proctime2")
  }

  /**
   * 处理时间3 定义动态表时，显示声明处理时间、ddl风格
   * create table sensor(
   *    id string,
   *    ts bigint,
   *    vc int,
   *    pt as PROCTIME()
   * )
   */
  @Test
  def proctime3(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env, default)

    tabEnv.executeSql(
      s"""create table sensor(
          | id string,
          | ts bigint,
          | vc int,
          | pt as PROCTIME()
          |) with (
          | 'connector'='filesystem',
          | 'path'='${CommonSuit.getFile("sensor/sensor.txt")}',
          | 'format'='csv'
          |)
         |""".stripMargin)

    // 处理时间
    val subTab = tabEnv.sqlQuery(s"select * from sensor")
    subTab.print()

    env.execute("proctime3")
  }

  /**
   * 事件时间1：流中定义好时间语义，创建动态表时使用新字段承接
   * $("et").rowtime()
   */
  @Test
  def rowtime1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env, default)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor](){
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    // 处理时间
    val sensor = tabEnv.fromDataStream(ds,$("id"),$("ts"),$("vc"),$("et").rowtime())
    val subTab = tabEnv.sqlQuery(s"select * from ${sensor}")
    subTab.print()

    env.execute("rowtime1")
  }

  /**
   * 事件时间2：流中定义好时间语义，创建动态表时使用已经存在字段承接
   * $("ts").rowtime()
   */
  @Test
  def rowtime2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env, default)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )
    val ds = env.addSource(new StreamSourceMock(seq,false))
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10))
        .withTimestampAssigner(new SerializableTimestampAssigner[WaterSensor](){
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    // 处理时间
    val sensor = tabEnv.fromDataStream(ds,$("id"),$("ts").rowtime(),$("vc"))
    val subTab = tabEnv.sqlQuery(s"select * from ${sensor}")
    subTab.print()

    env.execute("rowtime2")
  }

  /**
   * 事件时间3：ddl 中基于已有时间戳定义时间和水印
   * t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss'))
   * 1.ddl中事件时间必须是timestamp3类型；
   * 2.三种水印
   *  watermark for rowtime_col as rowtime_col 严格递增
   *  watermark for rowtime_col as rowtime_col - 'xx' timeUnit 乱序
   *  eg: watermark for rowtime_col as rowtime_col - '5' second
   */
  @Test
  def rowtime3(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val default = TableConfig.getDefault
    val tabEnv = StreamTableEnvironment.create(env, default)

    tabEnv.executeSql(
      s"""create table sensor (
         |  id string,
         |  ts bigint,
         |  vc int,
         |  t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - interval '5' second
         |) with (
         |  'connector'='filesystem',
         |  'path'='${CommonSuit.getFile("sensor/sensor.txt")}',
         |  'format'='csv'
         |)
         |""".stripMargin)

    // 处理时间
    val subTab = tabEnv.sqlQuery("select * from sensor")
    subTab.print()

    env.execute("rowtime2")
  }

  /**
   * Table API 风格开窗统计
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   * | op |                             id |            window_start |              window_end |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   * | +I |                       sensor_1 |     2020-12-09T15:33:10 |     2020-12-09T15:33:15 |          13 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |     2020-12-09T15:33:15 |           5 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |     2020-12-09T15:33:20 |          53 |
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   */
  @Test
  def tumblingWindow1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
        override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
      }))

    val table = tabEnv.fromDataStream(ds,$("id"),$("ts").rowtime(),$("vc")) // 声明事件时间
      .window(Tumble.over(lit(5).second()).on("ts").as("w")) // 定义滚动窗口，并取别名 lit： 过去的
      .groupBy($("id"),$("w"))
      .select($("id"),$("w").start().as("window_start"),$("w").end().as("window_end"),$("vc").sum().as("vc_sum"))
      .execute()

    table.print()

    env.execute("tumblingWindow")
  }

  /**
   * sql 风格开窗统计
   * TUMBLE(t,INTERVAL 窗口长度)
   * 注 TUMBLE 只能出现在group by中
   */
  @Test
  def tumblingWindow2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
          |  id string,
          |  ts bigint,
          |  vc int,
          |  t as to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')),
          |  watermark for t as t - interval '10' second
          |) with (
          | 'connector'='filesystem',
          | 'path'='${CommonSuit.getFile("sensor/sensor.txt")}',
          | 'format'='csv'
          |)
         |""".stripMargin)

    val table1 = tabEnv.executeSql(
      s"""
         |select id,
         | DATE_FORMAT(TUMBLE_START(t,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as window_start,
         | DATE_FORMAT(TUMBLE_END(t,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as window_end,
         |SUM(vc) as vc_sum
         |from sensor
         |group by id,TUMBLE(t,INTERVAL '10' SECOND)
         |""".stripMargin)

    table1.print()
  }

  /**
   * API 风格滑动窗口
   * Slide.over(窗口长度).every(滑动步长).on(水印时间).as(窗口别名)
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   * | op |                             id |            window_start |              window_end |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   * | +I |                       sensor_2 |     2020-12-09T15:33:05 |     2020-12-09T15:33:15 |           5 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |     2020-12-09T15:33:20 |          58 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:15 |     2020-12-09T15:33:25 |          53 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:05 |     2020-12-09T15:33:15 |          13 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:10 |     2020-12-09T15:33:20 |          13 |
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   */
  @Test
  def slideWindow1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992050L, 1),
      WaterSensor("sensor_2", 1607527992000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994050L, 11),
      WaterSensor("sensor_2", 1607527995500L, 5),
      WaterSensor("sensor_2", 1607527995550L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    val table = tabEnv.fromDataStream(ds,$("id"),$("ts").rowtime(),$("vc")) // 声明事件时间
      .window(Slide.over(lit(10).second()).every(lit(5).second()).on("ts").as("w")) // 定义滑动窗口 窗口长度10秒，间隔5秒滑动一次
      .groupBy($("id"),$("w"))
      .select($("id"),$("w").start().as("window_start"),$("w").end().as("window_end"),$("vc").sum().as("vc_sum"))
      .execute()

    table.print()

    env.execute("slidingWindow1")
  }

  /**
   * sql风格 滑动窗口
   *     水印时间   滑动步长         窗口长度
   * HOP(t,INTERVAL '5' SECOND,INTERVAL '10' SECOND)
   * 注：HOP(ts,slide,window) 只能出现在group by 中
   */
  @Test
  def slideWindow2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
       |  id string,
       |  ts bigint,
       |  vc int,
       |  t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),
       |  watermark for t as t - interval '5' second
       |) with (
       | 'connector'='filesystem',
       | 'path'='${CommonSuit.getFile("sensor/sensor2.txt")}',
       | 'format'='csv'
       |)
       |""".stripMargin)

//    tabEnv.executeSql("select * from sensor").print()

    val table1 = tabEnv.executeSql(
      s"""select id,
         |DATE_FORMAT(HOP_START(t,INTERVAL '5' SECOND,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as window_start,
         |DATE_FORMAT(HOP_END(t,INTERVAL '5' SECOND,INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') as window_end,
         |SUM(vc) as vc_sum
         |from sensor
         |group by id,HOP(t,INTERVAL '5' SECOND,INTERVAL '10' SECOND)
         |""".stripMargin)

    table1.print()
  }

  /**
   * API 风格：Session Gap Window 间隔超过3秒，就划分为不同窗口
   * Session.withGap(间隔长度).on(水印时间字段).as(窗口别名)
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   * | op |                             id |            window_start |              window_end |      vc_sum |
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   * | +I |                       sensor_2 |     2020-12-09T15:33:10 |     2020-12-09T15:33:13 |           2 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:12 |     2020-12-09T15:33:15 |           2 |
   * | +I |                       sensor_2 |     2020-12-09T15:33:14 |     2020-12-09T15:33:19 |          56 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:16 |     2020-12-09T15:33:19 |          11 |
   * | +I |                       sensor_1 |     2020-12-09T15:33:20 |     2020-12-09T15:33:23 |          11 |
   * +----+--------------------------------+-------------------------+-------------------------+-------------+
   */
  @Test
  def sessionGap1(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527992000L, 1),
      WaterSensor("sensor_2", 1607527990000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527996000L, 11),
      WaterSensor("sensor_2", 1607527995000L, 5),
      WaterSensor("sensor_2", 1607527995000L, 24),
      WaterSensor("sensor_2", 1607527996000L, 24),
      WaterSensor("sensor_1", 1607528000000L, 11),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false)).assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(
        new SerializableTimestampAssigner[WaterSensor]() {
          override def extractTimestamp(element: WaterSensor, recordTimestamp: Long): Long = element.ts
        }))

    val table = tabEnv.fromDataStream(ds,$("id"),$("ts").rowtime(),$("vc")) // 声明事件时间
      .window(Session.withGap(lit(3).second()).on("ts").as("w")) // 定义滑动窗口 窗口长度10秒，间隔5秒滑动一次
      .groupBy($("id"),$("w"))
      .select($("id"),$("w").start().as("window_start"),$("w").end().as("window_end"),$("vc").sum().as("vc_sum"))
      .execute()

    table.print()

    env.execute("sessionGap1")
  }

  /**
   * SQL 风格 session gap window
   * SESSION(t,INTERVAL 间隔时间)
   * 注：SESSION(t,INTERVAL '3' SECOND) 只能出现在group by中
   */
  @Test
  def sessionGap2(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    tabEnv.executeSql(
      s"""create table sensor(
         |  id string,
         |  ts bigint,
         |  vc int,
         |  t as to_timestamp(from_unixtime(ts/1000,'yyyy-MM-dd HH:mm:ss')),
         |  watermark for t as t - interval '5' second
         |) with (
         | 'connector'='filesystem',
         | 'path'='${CommonSuit.getFile("sensor/sensor3.txt")}',
         | 'format'='csv'
         |)
         |""".stripMargin)

    //    tabEnv.executeSql("select * from sensor").print()

    val table1 = tabEnv.executeSql(
      s"""select id,
         |DATE_FORMAT(SESSION_START(t,INTERVAL '3' SECOND),'yyyy-MM-dd HH:mm:ss') as window_start,
         |DATE_FORMAT(SESSION_END(t,INTERVAL '3' SECOND),'yyyy-MM-dd HH:mm:ss') as window_end,
         |SUM(vc) as vc_sum
         |from sensor
         |group by id,SESSION(t,INTERVAL '3' SECOND)
         |""".stripMargin)

    table1.print()
  }





}
