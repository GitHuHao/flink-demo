package com.bigdata.flink.source

import com.bigdata.flink.func.{SnapshotableSumFunction, StreamSourceMock}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.{AbstractManagedMemoryStateBackend, StateBackend}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.junit.Test

class StateBackendDemo {

  /**
   * 所有state记录在jobmanager内存中，checkpoint也记录在jobmanager内存
   * 访问速度最快、数据已丢失、容易导致jobmanager写挂了，只在测试使用，生产环境不用
   */
  @Test
  def memoryStateBackend(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 间隔2000ms执行一次chkp，维持EXACTLY_ONCE语义
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    // chkp超过1min，就放弃
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 两次chkp之间间隔至少保存500ms以上
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 最大并发chkp数为1，即同一时间不容许出现两个chkp记录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 允许cancel操作时，保存savepoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // checkpoint路径
    env.setStateBackend(new MemoryStateBackend(true))
    env.setParallelism(2)
    val seq = Seq(1,2,3,4,5,6)
    val ds = env.addSource(new StreamSourceMock[Int](seq))
    ds.map(new SnapshotableSumFunction).uid("aa").filter(_%2==0).uid("bb").print()
    env.execute("sum")
  }

  /**
   * 所有state记录在jobmanager内存，checkpoint写到文件系统中(异步写)
   * 具有内存级别访问速度，同时基于文件系统执行checkpoint，保证数据可靠性，可存储较大数量状态，需要对jobmanager配置HA，否则容易将jobmanager写挂了
   * 使用场景：分钟级别大窗口、大KV状态相关计算。jobmanager有可能写挂，需要配置HA，生产环境使用最多
   */
  @Test
  def fsStateBackend(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 间隔2000ms执行一次chkp，维持EXACTLY_ONCE语义
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    // chkp超过1min，就放弃
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 两次chkp之间间隔至少保存500ms以上
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 最大并发chkp数为1，即同一时间不容许出现两个chkp记录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 允许cancel操作时，保存savepoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // checkpoint路径
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/tutorial/fs_state_backend",true))
    env.setParallelism(2)
    val seq = Seq(1,2,3,4,5,6)
    val ds = env.addSource(new StreamSourceMock[Int](seq))
    ds.map(new SnapshotableSumFunction).uid("aa").filter(_%2==0).uid("bb").print()
    env.execute("sum")
  }

  /**
   * 本地状态存储在taskmanager的rocksDB数据库(内存+磁盘，kv结构nosql数据库)，checkpoint存储在远程文件系统中。
   * 适合执行超大规模窗口(天级别)、海量KV状态计算，可以增量执行checkpoint
   *
   *
   * redis 内存级别KV数据库，基于内存，因此不用考虑逻辑连续，存储不连续带来磁盘随机访问效率低下问题，太吃内存；
   * levelDB 基于磁盘的KV数据库，顺序写磁盘，key相邻的数据分部在一定连续空间，避免随机访问开销；
   * rocksDB 内存 + 磁盘（LSM）写入操作先记录WAL，最近写入操作优先存储在内存，然后周期性刷写到磁盘，以树结构组织数据，并周期性将小树合并到大树上，
   * 构建起一系列高度不同树，且合并树操作时异步执行。查询时先查内存，然后查小树，再查大树，直至找到，或扫描完不同高度树。
   *
   */
  @Test
  def rocksDBStateBackend(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 间隔2000ms执行一次chkp，维持EXACTLY_ONCE语义
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    // chkp超过1min，就放弃
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 两次chkp之间间隔至少保存500ms以上
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 最大并发chkp数为1，即同一时间不容许出现两个chkp记录
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 允许cancel操作时，保存savepoint
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // checkpoint路径
    env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop01:9000/flink/checkpoint/tutorial/rocks_db_state_backend").asInstanceOf[StateBackend])
    env.setParallelism(2)
    val seq = Seq(1,2,3,4,5,6)
    val ds = env.addSource(new StreamSourceMock[Int](seq))
    ds.map(new SnapshotableSumFunction).uid("aa").uid("bb").print()
    env.execute("sum")
  }

}
