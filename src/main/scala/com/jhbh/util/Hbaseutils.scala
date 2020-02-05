package com.jhbh.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

object Hbaseutils {

  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "bigdata-01:2181,bigdata-02:2181,bigdata-03:2181")

  def getConnection(): Connection = {
    return ConnectionFactory.createConnection(conf)
  }
}