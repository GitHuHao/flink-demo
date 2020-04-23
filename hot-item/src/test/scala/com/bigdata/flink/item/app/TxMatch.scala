package com.bigdata.flink.item.app

import com.bigdata.flink.item.bean.tx.{OrderEvent, ReceiptEvent}
import com.bigdata.flink.item.transform.tx.{TxMatchDetection, TxPayMatchByJoin}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.junit.Test

class TxMatch {

  @Test
  def byCoProcess(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val unmatchedPaysTag = new OutputTag[OrderEvent]("unmatchedPays")
    val unmatchedReceiptsTag = new OutputTag[ReceiptEvent]("unmatchedReceipts")

    val orderPath = getClass.getResource("/input/OrderLog.csv").getPath
    val receiptPath = getClass.getResource("/input/ReceiptLog.csv").getPath

    val orderEventStream = env.readTextFile(orderPath)
      .map{line =>
        val dataArray = line.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong*1000)
      }.filter(_.txId!="")
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.txId)

    val receiptEventStream = env.readTextFile(receiptPath)
      .map{line =>
        val dataArray = line.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong*1000)
      }.assignAscendingTimestamps(_.eventTime)
      .keyBy(_.txId)

    val matchedStream = orderEventStream.connect(receiptEventStream).process(new TxMatchDetection(unmatchedPaysTag,unmatchedReceiptsTag))

    matchedStream.getSideOutput(unmatchedPaysTag).print("unmatched pays")
    matchedStream.getSideOutput(unmatchedReceiptsTag).print("unmatched receipts")

    matchedStream.print("prcessed")

    env.execute("Orders match Receipts Job")

  }

  @Test
  def byIntervalJoin(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val orderPath = getClass.getResource("/input/OrderLog.csv").getPath
    val receiptPath = getClass.getResource("/input/ReceiptLog.csv").getPath

    val orderStream = env.readTextFile(orderPath)
      .map{line =>
        val dataArray = line.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong*1000)
      }.filter(_.txId!="")
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.txId)

    val receiptStream = env.readTextFile(receiptPath)
      .map{line =>
        val dataArray = line.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong*1000)
      }.assignAscendingTimestamps(_.eventTime)
      .keyBy(_.txId)

    orderStream.intervalJoin(receiptStream)
      .between(Time.seconds(-60),Time.seconds(60))
      .process(new TxPayMatchByJoin)
      .print()

    env.execute("Orders match Receipts by Interval Join")

  }




}
