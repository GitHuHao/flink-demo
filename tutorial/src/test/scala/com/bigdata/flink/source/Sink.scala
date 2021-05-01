package com.bigdata.flink.source

import com.bigdata.flink.model.WaterSensor
import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
import org.junit.Test
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.flink.api.common.functions.{IterationRuntimeContext, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.{Request, Requests}
import org.elasticsearch.common.xcontent.XContentType
import org.joda.time.DateTime

import java.sql.{Connection, Date, DriverManager, PreparedStatement}
import java.text.SimpleDateFormat
import java.util

class Sink {

  /**
   * 自定义Sink函数，优先加载def invoke(value: Int, context: SinkFunction.Context)的实现
   */
  @Test
  def print(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.fromElements(1, 2, 3).addSink(
      new SinkFunction[Int] {

        override def invoke(value: Int): Unit = {
          println(value)
        }

        override def invoke(value: Int, context: SinkFunction.Context): Unit = {
          val ts = context.currentProcessingTime()
          println(ts, value)
        }

      })
    env.execute("printSink")
  }

  @Test
  def kafkaSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.readTextFile(CommonSuit.getFile("wc/1.txt"))
      .addSink(new FlinkKafkaProducer[String](
        "hadoop01:9092,hadoop02:9092,hadoop03:9092",
        "test",
        new SimpleStringSchema()))
    env.execute("kafkaSink")
  }

  // hset 方式写出到redis
  // redis-cli --raw 查看
  @Test
  def redisSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)

    val config = new FlinkJedisPoolConfig.Builder().setMaxIdle(2).setMinIdle(0)
      .setHost("hadoop01")
      .setPort(6379)
      .setTimeout(1000 * 10).build()

    env.fromElements(
      WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527996000L, 24),
    ).addSink(new RedisSink[WaterSensor](config, new RedisMapper[WaterSensor] {
      override def getCommandDescription: RedisCommandDescription = {
        new RedisCommandDescription(RedisCommand.HSET, "sensor")
      }

      override def getKeyFromData(t: WaterSensor): String = {
        t.id
      }

      override def getValueFromData(t: WaterSensor): String = {
        JSON.toJSONString(t, new SerializeConfig(true))
      }
    }))
    env.execute("redisSink")
  }

  // 写入es curl http://hadoop01:9200/sensor/_doc/_search
  @Test
  def elasticSink(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val elasticHost = util.Arrays.asList(
      new HttpHost("hadoop01", 9200),
      new HttpHost("hadoop02", 9200),
      new HttpHost("hadoop03", 9200)
    )
    val ds = env.fromElements(
      WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527996000L, 24)
    )
    val builder = new ElasticsearchSink.Builder[WaterSensor](
      elasticHost,
      new ElasticsearchSinkFunction[WaterSensor]() {
        override def process(t: WaterSensor, runtimeContext: RuntimeContext,
                             requestIndexer: RequestIndexer): Unit = {
          val indexer = Requests.indexRequest("sensor")
            .`type`("_doc")
            .id(t.id)
            .source(JSON.toJSONString(t, new SerializeConfig(true)), XContentType.JSON)
          requestIndexer.add(indexer)
        }
      })
    builder.setBulkFlushMaxActions(3) // TODO 无界流需要设置bulk 参数
    val elasticSink = builder.build()

    ds.addSink(elasticSink
    )

    env.execute("elasticSink")
  }

  @Test
  def mysqlSink(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds = env.fromElements(
      WaterSensor("sensor_1", 1607527992000L, 20),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_2", 1607527992000L, 22),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527996000L, 24)
    )

    ds.addSink(new RichSinkFunction[WaterSensor] {

      private var conn:Connection = _
      private var ps: PreparedStatement = _

      private val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

      override def open(parameters: Configuration): Unit = {
        val sql = "insert into sensor(id,ts,vc) values(?,?,?) on duplicate key update ts=values(ts),vc=values(vc)"
        conn = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/test?useSSL=false", "test", "test")
        ps = conn.prepareStatement(sql)
      }

      override def invoke(value: WaterSensor): Unit = {
        ps.setString(1,value.id)
        ps.setString(2,sdf.format(new Date(value.ts)))
        ps.setInt(3,value.vc)
        ps.execute()
      }

      override def close(): Unit = {
        if(ps!=null){
          ps.close()
        }
        if(conn!=null){
          conn.close()
        }
      }
    })

    env.execute("mysqlSink")
  }


}
