package com.bigdata.flink.item.transform.uv

import java.text.SimpleDateFormat
import java.util.Date

import com.bigdata.flink.item.bean.behavior.UserBehavior
import com.bigdata.flink.item.bean.uv.UvCount
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

class UvCountWithBloomByElement extends ProcessWindowFunction[UserBehavior,UvCount,Long,TimeWindow]{

  private lazy val jedis = new Jedis("hadoop01", 6379)
  private lazy val bloom = new Bloom(1<<29)
  private val sdf = new SimpleDateFormat("yyyy年MM月dd日 HH时")

  override def process(key: Long, context: Context, elements: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    val end = context.window.getEnd
    val storeKey = end.toString

    val hit = jedis.hget("count", storeKey) // hash结构存 uv 数值
    var count = if (hit == null) 0l else hit.toLong

    val userId = elements.last.userId

    val offset = bloom.hash(userId.toString, 61)

    val existed = jedis.getbit(storeKey, offset) // bit 结构 判断uv数值是否存在指定uid

    if(!existed){
      count += 1
      jedis.setbit(storeKey,offset,true)
      jedis.hset("count",storeKey,count.toString)
    }

    val time = sdf.format(new Date(end))
    out.collect(UvCount(time,count))

  }
}
