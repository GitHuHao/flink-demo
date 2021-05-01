package com.bigdata.flink.source

import com.bigdata.flink.func.{SnapshotableSumFunction, StreamSourceMock}
import com.bigdata.flink.model.WaterSensor
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, functions}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.junit.Test

import java.util
import scala.collection.JavaConversions.asScalaIterator

/**
 * 1. state 状态：
 *  键控状态: key相同元素共享状态；ListState、MapState、ValueState、ReduceState、AggregateState、FoldState
 *  算子状态: 每个元素记录自己状态，常用于source、sink操作中记录offset信息，确保exactly-once语义； ListState、BroadcastState
 *
 *  算子每处理一个元素，对应都会记录一个状态，所不同的时，键控状态是更新键的状态，算子状态是增量记录一个新状态。状态记录在taskmanager中执行，
 *  然后jobmanager周期性触发checkpoint操作，将taskmanager的状态，合并到checkpoint记录中，方便故障快速恢复。
 *  即:
 *  每处理一个元素记录一次         周期性执行
 *  taskmanager(state) -> jobmanager(checkpoint)
 *
 *  使用场景：
 *    UV指标去重、最近1小时指标统计、连续指标变化状态监控。
 *
 *  状态后端：
 *  MemoryStateBackend 内存，小kv，小数据量，生产环境不用；
 *  FsStateBackend 文件，大kv，大数据量，使用较多；
 *  RocksDBStateBackend 文件，海量数据，以数据库文件方式组织数据，允许增量checkpoint，数据同时会远程上传一份到hdfs。
 *
 * 2.checkpoint 保存点
 *   jobmanager checkpoint协调器周期性协调taskmanager回传state，生成保存点文件，方便出故障时，重启快速恢复。
 *   checkpoint默认关闭，若处于关闭状态，即使设置了重启策略，也不执行，因此重启建立在checkpoint基础上；
 *   checkpoint需要手动开启，默认重启策略为固定间隔重启
 *
 *   常见重启策略：
 *   fixedDelayRestartStrategy 固定间隔重启
 *   failureRatioRestartStrategy 失败率重启
 *   callbackRestartStrategy 回调重启
 *   noRestart 不重启
 *
 *   checkpoint路径
 *   MemoryStateBackend
 *
 *   checkpoint 自动周期性执行，仅在故障发生，需要重启时使用，代码升级后无法使用。且会过期。
 *
 *  3.savepoint 暂存点
 *  停机时指定路径保存状态，重新运行时从指定保存点启动，恢复之前状态。
 *  版本升级后，也能够使用，不会过期。
 *
 *  注：为适应代码修改，强烈建议给所有算子指定uid。
 *
 */
class State {

  // 算子状态
  @Test
  def operatorState(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 间隔2000ms执行一次chkp，维持EXACTLY_ONCE语义
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    // chkp超过1min，就放弃
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 两次chkp之间间隔至少保存500ms以上
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 最大并发chkp数为1，即同一时间不容许出现两个chkp记录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 开启在 job 中止后仍然保留的 externalized checkpoints
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // checkpoint路径
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/tutorial/operator_state"))
    env.setParallelism(2)
    val seq = Seq(1,2,3,4,5,6)
    val ds = env.addSource(new StreamSourceMock[Int](seq))
    ds.map(new SnapshotableSumFunction).uid("aa").filter(_%2==0).uid("bb").print()
    env.execute("sum")
  }

  /**
   * 广播状态：将低流量流广播，其余正常业务流与其连接，在BroadcastProcessFunction中，通过关广播流引入配置变更信息，动态调整业务处理逻辑
   * 举例：flink cds 配置表指导binlog动态分流
   */
  @Test
  def broadcastState(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.socketTextStream("localhost",9001)
    val ds2 = env.socketTextStream("localhost",9002)

    val broadcastStateDesc = new MapStateDescriptor("config-ds", classOf[String], classOf[String])
    val broadcastDS = ds1.broadcast(new MapStateDescriptor("config-ds", classOf[String], classOf[String]))

    val connectDS = ds2.connect(broadcastDS)

    connectDS.process(new BroadcastProcessFunction[String,String,String]{
      override def processElement(in1: String, readOnlyContext: BroadcastProcessFunction[String, String, String]#ReadOnlyContext,
                                  collector: Collector[String]): Unit = {
        val config = readOnlyContext.getBroadcastState(broadcastStateDesc)
        if(config.contains("switch") && config.get("switch").equals(in1)){
          collector.collect(in1)
        }
      }

      override def processBroadcastElement(in2: String, ctx: BroadcastProcessFunction[String, String, String]#Context,
                                           collector: Collector[String]): Unit = {
        val config = ctx.getBroadcastState(broadcastStateDesc)
        config.put("switch",in2)
        collector.collect(s"switch config to ${in2}")
      }
    }).print()

    env.execute("broadcastState")
  }

  /**
   * 水位线差距超过10，就报警
   */
  @Test
  def valueState(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq( WaterSensor("sensor_1", 1607527992000L, 2),
      WaterSensor("sensor_1", 1607527994000L, 13),
      WaterSensor("sensor_2", 1607527992000L, 1),
      WaterSensor("sensor_1", 1607527996000L, 22),
      WaterSensor("sensor_2", 1607527996000L, 24))

    val ds = env.addSource(new StreamSourceMock(seq,false))
    val outputTag = new OutputTag[String]("alert")

    val mainDS = ds.keyBy(_.id)
      .process(new ProcessFunction[WaterSensor,WaterSensor]{
        private var state:ValueState[WaterSensor] = _

        override def open(parameters: Configuration): Unit = {
          state = getRuntimeContext.getState(new ValueStateDescriptor("",classOf[WaterSensor]))
        }

        override def processElement(i: WaterSensor, context: ProcessFunction[WaterSensor, WaterSensor]#Context,
                                    collector: Collector[WaterSensor]): Unit = {
          val last = state.value()
          if(last!=null && Math.abs(last.vc-i.vc)>10){
            context.output(outputTag,s"${last} -> ${i}")
          }
          state.update(i)
          collector.collect(i)
        }
      })

    mainDS.getSideOutput(outputTag).print()

    env.execute("valueState")
  }

  /**
   * 2个最高水位线
   */
  @Test
  def listState(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 2),
      WaterSensor("sensor_1", 1607527994000L, 13),
      WaterSensor("sensor_1", 1607527994000L, 12),
      WaterSensor("sensor_1", 1607527994000L, 14),
      WaterSensor("sensor_1", 1607527994000L, 24),
      WaterSensor("sensor_1", 1607527994000L, 21),
      WaterSensor("sensor_1", 1607527994000L, 12),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false))
    val outputTag = new OutputTag[String]("alert")

    val mainDS = ds.keyBy(_.id)
      .process(new ProcessFunction[WaterSensor,WaterSensor]{
        private var state:ListState[WaterSensor] = _

        override def open(parameters: Configuration): Unit = {
          state = getRuntimeContext.getListState(new ListStateDescriptor("high-vc",classOf[WaterSensor]))
        }

        override def processElement(i: WaterSensor, context: ProcessFunction[WaterSensor, WaterSensor]#Context,
                                    collector: Collector[WaterSensor]): Unit = {
          val old = state.get().iterator().toList
          state.add(i)
          val temp = new util.ArrayList[WaterSensor]()
          state.get().forEach(i => temp.add(i))
          temp.sort((w1, w2) => w2.vc - w1.vc)
          if(temp.size>3){
            temp.remove(3)
          }

          if(temp.size()==3){
            var j = 0
            while(j<temp.size()){
              if(old(j) != temp.get(j)){
                state.update(temp)
                j = temp.size()
                context.output(outputTag,temp.toArray().mkString(","))
              }
              j += 1
            }
          }

          collector.collect(i)
        }
      })

    mainDS.getSideOutput(outputTag).print()

    env.execute("valueState")
  }

  /**
   * 使用场景：UV去重、KV匹配
   * 求传感器不同水位线
   */
  @Test
  def mapState(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val seq = Seq(
      WaterSensor("sensor_1", 1607527992000L, 2),
      WaterSensor("sensor_1", 1607527994000L, 2),
      WaterSensor("sensor_1", 1607527994000L, 3),
      WaterSensor("sensor_1", 1607527994000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 2),
      WaterSensor("sensor_2", 1607527994000L, 3),
      WaterSensor("sensor_2", 1607527994000L, 4),
    )

    val ds = env.addSource(new StreamSourceMock(seq,false))
    val outputTag = new OutputTag[String]("alert")

    val mainDS = ds.keyBy(_.id)
      .process(new ProcessFunction[WaterSensor,WaterSensor]{
        private var state:MapState[Int,String] = _

        override def open(parameters: Configuration): Unit = {
          state = getRuntimeContext.getMapState(new MapStateDescriptor("vc",classOf[Int],classOf[String]))
        }

        override def processElement(i: WaterSensor, context: ProcessFunction[WaterSensor, WaterSensor]#Context,
                                    collector: Collector[WaterSensor]): Unit = {
          if(!state.contains(i.vc)){
            state.put(i.vc,null)
            context.output(outputTag,s"${i.id} ${i.vc}")
          }
          collector.collect(i)
        }
      })

    mainDS.getSideOutput(outputTag).print()

    env.execute("mapState")
  }

}
