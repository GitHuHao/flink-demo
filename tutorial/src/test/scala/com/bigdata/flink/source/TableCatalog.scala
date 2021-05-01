package com.bigdata.flink.source

import com.bigdata.flink.suit.CommonSuit
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.junit.Test

class TableCatalog {

  /**
   * hive catalog 除了可以访问hive数据外，更重要的是可操作元数据（建表、改表、新建分区等）
   * 使用前需要启动metastore (bin/metaStart)
   * nohup hive --service metastore >/dev/null 2>&1 &
   * lsof -i :9083
   */
  @Test
  def hiveCatalog(): Unit ={
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tabEnv = StreamTableEnvironment.create(env)

    val catalogName="hiveCatalog"
    val defaultDatabase = "default"
    val hiveConfigDir = CommonSuit.getFile("hive") // 自己补全  hive-site.xml
    val catalog = new HiveCatalog(catalogName, defaultDatabase, hiveConfigDir)

    tabEnv.registerCatalog(catalogName,catalog)

    tabEnv.useCatalog(catalogName)
    tabEnv.useDatabase(defaultDatabase)

    tabEnv.executeSql(" select * from log_orc limit 5").print()
  }

  // TODO 目前只支持PG，通过修改源码可以支持mysql
  @Test
  def jdbcCatalog(): Unit ={

  }

  // TODO 自定义catalog
  @Test
  def customerCatalog(): Unit ={

  }


}
