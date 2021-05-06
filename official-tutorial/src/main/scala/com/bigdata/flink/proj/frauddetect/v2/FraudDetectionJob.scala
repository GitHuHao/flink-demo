package com.bigdata.flink.proj.frauddetect.v2

import com.bigdata.flink.proj.frauddetect.common.AlertSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
 * 欺诈交易识别：
 * 小金额取现(1元)后面，紧跟一个大金额取现（500元）
 *
 * 通过键控状态管理每个账户交易信息，open 函数初始化键控状态ValueState[T]，且不能再open中执行 update操作。
 * 每接收一个元素先查看之前记录的状态是否为null （只会存在null、true）两个值，之前发生过小金额体现对应 !=null，然后比对当前取现金额是否超过大额交易阈值，超过就触发报警。
 * 然后清空状态，再比对当前取现是否是小额交易，如果是就将状态设置为true。
 *
 * flink 状态：
 * 键控状态: ListState、ValueState、MapState、ReduceState、FoldState、AggregatingState
 * 1）只适用于KeyedStream；
 * 2）一个Key对应一个状态，一个算子可以对应多个子任务，每个子任务处理多个key，即每个算子子任务维护多个状态；（与算子子任务无关，与算子子任务处理的ke个数有关）；
 * 3）并发横向扩展后，key在算子子任务直接重新分配，状态也跟着key一起迁移；
 * 4）重写RickFunction，通过RuntimeContext进行访问；
 *
 * 算子状态: ListState、BroadcastState
 * 1）常用语Source上如FlinkKafkaConsumer；
 * 2）一个算子有几个实例在执行，就对应有几个状态(一个算子子任务对应一个状态)；
 * 3）并发横向扩展后，状态在并发实例直接平均分配，或全量拷贝；
 * 4）实现CheckpointedFunction或ListCheckpointed接口时可以访问。
 *
 *
 */
object FraudDetectionJob {

  def main(args: Array[String]): Unit = {
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    val txDS = env.addSource(new TransactionSource)
      .uid("tx-source")// uid和name是为了在checkpoint中标记算子的状态。
      .name("tx-source")

    val detectedDS = txDS.keyBy(_.getAccountId).process(new FraudDetector())
      .uid("fraud-detector")
      .name("fraud-detector")

    detectedDS.addSink(new AlertSink)
      .uid("alert-sink")
      .name("alert-sink")

    env.execute("FraudDetectionJob")
  }

}
