package com.bigdata.flink.savepoint

import com.bigdata.flink.func.SnapshotableSumFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._

/**
 * yarn-application 模式提交作业
 *
 * 1）启动hadoop集群：hadoopStart
 * 2）启动hadoop01节点：nc 9001
 * 3）第一次常规启动：
 * bin/flink run-application -t yarn-application -j ../job/streaming-wordcount-jar-with-dependencies.jar -c com.bigdata.flink.savepoint.RecoverFromSavepoint --host hadoop01 --port 9001
 * 4）访问yarn-webui: http://hadoop01:8081 > FlinkApplication > 跳转Flink WebUI
 * 5) nc 输入 100 200 300 400 -> 观察 Flink WebUI 累计到 1000
 * 6）设置保存点，并停止进程（因为是在yarn上运行，因此必须带-yid application_1618823695517_0005，从yarn webUI找的）
 * bin/flink cancel -s hdfs://hadoop01:9000/flink/savepoint/tutorial/operator_state de6b8ad2d3030e6dacb594b0f014803f -yid application_1618823695517_0005
 * 7) 从保存点重新启动运行
 * bin/flink run-application -t yarn-application -s hdfs://hadoop01:9000/flink/savepoint/tutorial/operator_state/savepoint-de6b8a-e0ab022eb92b -j ../job/streaming-wordcount-jar-with-dependencies.jar -c com.bigdata.flink.savepoint.RecoverFromSavepoint --host hadoop01 --port 9001
 * 8）nc继续输入500 -> 观察 Flink WebUI 累计到 1500
 * 结论：基于savepoint状态恢复成功
 */
object RecoverFromSavepoint {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    val host = params.get("host","localhost")
    val port = params.getInt("port",9001)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.enableCheckpointing(2000,CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(60000L)
    // 开启在 job 中止后仍然保留的 externalized checkpoints
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env.setStateBackend(new FsStateBackend("hdfs://hadoop01:9000/flink/checkpoint/tutorial/operator_state"))
    env.setParallelism(1)
    val ds = env.socketTextStream(host,port).map(_.trim.toInt)
    ds.map(new SnapshotableSumFunction).uid("map").print()
    env.execute("sum")
  }

}
